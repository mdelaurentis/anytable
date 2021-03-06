;; Anytable core library

(ns com.mdelaurentis.anytable
  (:import [java.io File]
           [java.net URI MalformedURLException URL])
  (:require [clojure.contrib.duck-streams :as streams])
  (:use [clojure.contrib sql command-line]))

(defmulti open-reader
  "Returns a copy of the given table that is open for reading.  You
must call close on the resulting reader when you're done reading from
it."
  :type)

(defmulti open-writer 
    "Returns a copy of the given table that is open for writing.  You
must call close on the resulting writer when you're done writing to
it."
  :type)

(defmulti close
  "Closes the reader and writer on the given table."
  :type)

(defmulti write-row
  "Writes a single row (vector of objects) to the given table writer.
Returns the writer as it exists after the row was written."
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

(defmulti enhance-spec 
  :type)

(def table-types (ref {}))

(defn add-type 
  "Adds a table type.  type should be a the keyword identifying the
type.  Parent, if supplided, must be a keyword identifying the parent
type of this type.  This type will inherit any default values from its
parent.  default, if supplied, must be a default configuration map for
the table."
  ([type]
     (add-type type nil))
  ([type default] 
     (dosync (alter table-types assoc type 
                    (with-meta (assoc default :type type) ^default))))
  ([type parent default]
     (derive type parent)
     (let [derived-default (merge (table-types parent) default)
           derived-meta    (merge-with (fn [a b]
                                         (if (and (map? a) (map? b))
                                           (merge a b)
                                           b))
                                       ^(table-types parent) ^default)]
       (add-type type (with-meta derived-default derived-meta)))))

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

(defmethod open-reader :default [spec]
  (throw (Exception. (str "No open-reader implementation for " spec))))

;; Most readers will want record-seq to be based on row-seq
(defmethod record-seq :default [table-spec]
  (let [hs (headers table-spec)]
    (map (partial zipmap hs) (row-seq table-spec))))

;; Most readers will want write-record to be based on write-row
(defn write-record [t record]
  (write-row t (map record (headers t))))

(defmethod enhance-spec :default [spec]
  spec)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; In-memory vector of vectors

(add-type ::vectors 
          #^{:doc "An in-memory table made up of a vector of vectors."}
          {:rows []})

(defn vector-table [& options] 
  (merge (table-types ::vectors) 
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

(add-type ::flat-file
          #^{:doc "Abstract type for any flat text file."
             :key-doc {:location "A URL or file locating the table."}
             :abstract true}
          {:location nil})

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
  (when-let [r (:reader t)] 
    (when (not (= *in* r))
      (.close r)))
  (when-let [w (:writer t)]
    (when (not (= *out* w))
      (.close w))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Delimited flat files

(add-type ::tab ::flat-file
          #^{:doc "Tab-delimited flat files."
             :key-doc {:delimiter "String used to delimit fields."
                       :location  "The file or URL where the table is located."}}
          {:delimiter "\t"})

(defn tab-table [loc & options]
  (merge
   (assoc (table-types ::tab)
     :location loc)
   (apply hash-map options)))

(defmethod parse-row ::tab [spec line]
  (vec (.split line (:delimiter spec))))

(defmethod format-row ::tab [spec row]
  (apply str (interpose (:delimiter spec) row)))

(defmethod open-reader ::tab [table-spec]
  (let [rdr     (if-let [loc (:location table-spec)]
                  (streams/reader loc)
                  (streams/reader *in*))
        headers (parse-row table-spec (first (line-seq rdr)))]
    (assoc table-spec
      :headers headers
      :reader  rdr)))

(defmethod open-writer ::tab [table-spec]
  (let [res (assoc table-spec
              :writer (if-let [loc (:location table-spec)]
                        (streams/writer loc)
                        *out*))]
    (write-row res (headers table-spec))
    res))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Fixed-width flat files

(add-type ::fixed-width ::flat-file
          #^{:doc "Fixed-width flat files."
             :key-doc {:headers "Vector of column headers."
                       :widths "Vector of widths of each column (integers)."
                       :bounds "Start and end position of each column.  You can specify either this or :widths."}}
          {:headers nil
           :widths nil
           :bounds nil})

(defmethod enhance-spec ::fixed-width [spec]
  (assoc spec 
    :bounds (reduce (fn [bs w] (conj bs (+ (last bs) w)))
                    [0]
                    (:widths spec))))

(defn fixed-width-table [loc & cols]
  (let [cols    (partition 2 cols)
        headers (map first cols)
        widths  (map second cols)]
    (enhance-spec (assoc (table-types ::fixed-width)
                    :location loc
                    :headers  headers
                    :widths   widths))))

(defmethod open-reader ::fixed-width [spec]
  (let [rdr (streams/reader (:location spec))]
    (assoc spec
      :reader rdr)))

(defmethod open-writer ::fixed-width [table-spec]
  (let [wtr (if-let [loc (:location table-spec)]
              (streams/writer loc)
              *out*)]
    (assoc table-spec 
      :writer wtr
      :format (apply str (map #(format "%%-%ds" %) (:widths table-spec))))))

(defmethod parse-row ::fixed-width [spec line]
  (for [[start end] (partition 2 1 (:bounds spec))]
    (do
      (.trim (.substring line start end)))))

(defmethod format-row ::fixed-width [spec row]
  (apply format (:format spec) row))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; JDBC

(add-type ::jdbc-table
          #^{:doc "A table in a database."
             :key-doc {:table-name "The name of the table in the database."}}
          {:table-name nil})

(defn jdbc-table [& options]
  (merge (table-types ::jdbc)
         (apply hash-map options)))

(defn create [spec]
  (with-connection spec
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; HSQLDB

(add-type ::hsqldb ::jdbc
          #^{:doc "HSQLDB table."
             :key-doc {:classname "The JDBC driver classname"}}
          {:classname   "org.hsqldb.jdbc.JDBCDataSource"
           :subprotocol "hsqldb"
           :subname     "hsql"})

(defn hsqldb-table [& options]
  (merge (table-types ::hsqldb)
         (apply hash-map options)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Functions for creating an anytable spec from Strings, Files, URIs, and maps
;;

(def default-in-spec (table-types ::tab))

(def default-out-spec (table-types ::tab))

(defn guess-type [location]
  (if (.endsWith (str location) ".tab")
    (tab-table location)))

(defn str-to-type [type]
   (keyword "com.mdelaurentis.anytable" (str type)))

(defn error [& msgs]
  (throw (Exception. (apply str msgs))))

(defn validate-map-spec [spec]
  (and (map? spec)
       (table-types (:type spec))
       spec))

(defn parse-str-map-spec [spec]
  (and (string? spec)
       (.startsWith (.trim spec) "{")
       (let [spec (read-string spec)
             spec (zipmap (keys spec) (map #(if (symbol? %) (str %) %) (vals spec)))
             type (str-to-type (:type spec))]
         (merge (table-types type) (assoc spec :type type)))))

(defn parse-location [loc]
  (let [parsed-loc (or (and (or (instance? File loc) 
                                (instance? URI loc))
                            (.endsWith (str loc) ".tab")
                            loc)
                       (and (string? loc)
                            (try (URL. loc)
                                 (catch MalformedURLException e
                                   (File. loc)))))]
    (tab-table parsed-loc)))

(defn anytable-spec [spec]
  (enhance-spec
   (or (validate-map-spec spec)
       (parse-str-map-spec spec)
       (parse-location spec)
       (error "Couldn't build a table representation from "
              spec ".  Please provide either a map with a :type field of one of "
              (keys @table-types) 
              ", or a URL or file name pointing to a tab-delimited file."))))
