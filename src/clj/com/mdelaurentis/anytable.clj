(ns com.mdelaurentis.anytable
  (:require [clojure.contrib.duck-streams :as streams])
  (:use [clojure.contrib sql]))

(defmulti open-reader
  "Opens the specified table for reading."
  :type)

(defmulti open-writer 
  "Opens the specified table for writing."
  :type)

(defmulti close
  "Close the reader and writer on the given table."
  :type)

(defmulti write-row
  "Writes a single row (vector of objects) to the given table writer."
  (fn [wtr row]
    (:type wtr)))

(defmulti row-seq
  "Returns a lazy sequence of rows (vectors of objects) from the given
  table reader."
  :type)

(defmulti delete 
  "Delete the specified table."
  :type)

(defn headers [spec]
     "Returns a vector of column names for the given table."
     (:headers spec))

(defmulti record-seq 
  "Returns a lazy sequence of records as maps from the given table."
  :type)

(defmacro with-reader [[name spec] & body]
  `(let [~name (open-reader ~spec)]
     (try ~@body
          (finally 
           (close ~name)))))

(defmacro with-writer [[name spec] & body]
  `(let [~name (open-writer ~spec)]
     (try ~@body
          (finally 
           (close ~name)))))

(defn copy [in out]
  (with-reader [rdr in]
    (with-writer [wtr (assoc out :headers (:headers rdr))]
      (reduce write-row wtr (row-seq rdr)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Default implementations

;; It's easy to forget to return the table spec from write-row, which
;; will result in the default method getting called, so warn when
;; that happens.
(defmethod write-row :default [spec row]
  (throw (Exception. (str "No write-row implementation for " spec))))

;; Most readers will want record-seq to be based on row-seq
(defmethod record-seq :default [table-spec]
  (let [hs (headers table-spec)]
    (map (partial zipmap hs) (row-seq table-spec))))

;; Most readers will want write-record to be based on write-row
(defn write-record [t record]
  (write-row t (map record (headers t))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; In-memory vector of vectors

(defn vector-table [& options] 
  (merge {:type ::vectors
          :rows []} 
         (apply hash-map options)))

(defmethod open-reader ::vectors [t] t)
(defmethod open-writer ::vectors [t] t)
(defmethod close ::vectors [t])

(defmethod row-seq ::vectors [t] (:rows t))
(defmethod write-row ::vectors [t row]
  (assoc t
    :rows (conj (:rows t) row)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Flat text files

(defmulti parse-row (fn [spec line]
                     (:type spec)))
(defmulti format-row (fn [spec row]
                       (:type spec)))

(defmethod row-seq ::flat-file [t]
  (map (partial parse-row t) (line-seq (:reader t))))

(defmethod write-row ::flat-file [t row]
  (binding [*out* (:writer t)]
    (println (format-row t row)))
  t)

(defmethod close ::flat-file [t]
  (when-let [r (:reader t)] (.close r))
  (when-let [w (:writer t)] (.close w)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Delimited flat files

(derive ::tab ::flat-file)

(defn tab-table [loc & options]
  (merge
   {:type ::tab
    :location loc
    :delimiter "\t"}
   (apply hash-map options)))

(defmethod parse-row ::tab [spec line]
  (.split line (:delimiter spec)))

(defmethod format-row ::tab [spec row]
  (apply str (interpose (:delimiter spec) row)))

(defmethod open-reader ::tab [table-spec]
  (let [rdr     (streams/reader (:location table-spec))
        headers (parse-row table-spec (first (line-seq rdr)))]
    (assoc table-spec
      :headers headers
      :reader  rdr)))

(defmethod open-writer ::tab [table-spec]
  (let [wtr (streams/writer (:location table-spec))]
    (binding [*out* wtr]
      (println (format-row table-spec (headers table-spec))))
    (assoc table-spec :writer wtr)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Fixed-width flat files

(derive ::fixed-width ::flat-file)

(defn fixed-width-table [loc & cols]
  (let [cols    (partition 2 cols)
        headers (map first cols)
        widths  (map second cols)
        bounds (reduce (fn [bs w] (conj bs (+ (last bs) w)))
                       [0]
                       widths)]
    {:type ::fixed-width
     :location loc
     :headers  headers
     :widths   widths
     :bounds   bounds}))

(defmethod open-reader ::fixed-width [spec]
  (let [rdr (streams/reader (:location spec))]
    (assoc spec
      :reader rdr)))

(defmethod parse-row ::fixed-width [spec line]
  (for [[start end] (partition 2 1 (:bounds spec))]
    (.trim (.substring line start end))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; JDBC

(defn jdbc-table [& options]
  (merge {:type ::jdbc}
         (apply hash-map options)))

(defn create [spec]
  (with-connection spec
    (try (drop-table (:table-name spec))
         (catch Exception e))
    (apply create-table (:table-name spec) 
           (:column-specs spec))))

(defmethod close ::jdbc [spec]
  (when-let [rs   (:resultset spec)]  (.close rs))
  (when-let [stmt (:statement spec)]  (.close stmt))
  (when-let [con  (:connection spec)] (.close con)))

(defmethod open-reader ::jdbc [spec]
  (let [con  (clojure.contrib.sql.internal/get-connection spec)
        stmt (.createStatement con)
        rs   (.executeQuery stmt (str "SELECT * FROM " (:table-name spec)))
        md   (.getMetaData rs)]
    (assoc spec
      :headers (map #(.getColumnName md (inc %)) 
                    (range 0 (.getColumnCount md)))
      :connection con
      :statement stmt
      :resultset rs)))

(defn insert-sql [spec]
  (format "INSERT INTO %s VALUES(%s)",
          (:table-name spec)
          (apply str (interpose ", " (repeat (count (:column-specs spec)) "?")))))

(defmethod open-writer ::jdbc [spec]
  (create spec)  
  (let [con  (clojure.contrib.sql.internal/get-connection spec)
        stmt (.prepareStatement con (insert-sql spec))]
    (assoc spec
      :connection con
      :statement stmt)))

(defmethod write-row ::jdbc [spec row]
  (let [stmt (:statement spec)]
    (doseq [[index value] (map vector (iterate inc 1) row)]
      (.setObject stmt index value))
    (.execute stmt))
  spec)

(defmethod row-seq ::jdbc [rdr]
  (map vals (record-seq rdr)))

(defmethod record-seq ::jdbc [rdr]
  (resultset-seq (:resultset rdr)))

;; HSQLDB

(defn hsqldb-table [& options]
  (apply jdbc-table
         :classname   "org.hsqldb.jdbc.JDBCDataSource"
         :subprotocol "hsqldb"
         :subname     "hsql"
         options))

