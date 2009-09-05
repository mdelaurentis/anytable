(ns com.mdelaurentis.anytable
  (:import [java.io File])
  (:require [clojure.contrib.duck-streams :as streams])
  (:use [clojure.contrib sql command-line])
  (:gen-class))

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

(def table-types (ref {}))

(defn add-type 
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
  (.split line (:delimiter spec)))

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

(defn fixed-width-table [loc & cols]
  (let [cols    (partition 2 cols)
        headers (map first cols)
        widths  (map second cols)
        bounds (reduce (fn [bs w] (conj bs (+ (last bs) w)))
                       [0]
                       widths)]
    (assoc (table-types ::fixed-width)
      :location loc
      :headers  headers
      :widths   widths
      :bounds   bounds)))

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
    (.trim (.substring line start end))))

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

(def default-in-spec (table-types ::tab))

(def default-out-spec (table-types ::tab))

(defn guess-type [location]
  (let [file (File. location)]))

(defn str-to-type [type]
   (keyword "com.mdelaurentis.anytable" (str type)))

(defn parse-spec [spec-str]
  (let [spec (read-string spec-str)
        spec (zipmap (keys spec) (map #(if (symbol? %) (str %) %) (vals spec)))
        type (str-to-type (:type spec))]
    (merge (table-types type) (assoc spec :type type))))

(defmulti main (fn [cmd & args]
                 cmd))

(defmethod main :cp [cmd & args]
  (when (not (= 2 (count args)))
    (throw (Exception. "Usage: anytable cp <in-spec> <out-spec>")))
  (let [[in out] args]
    (copy (parse-spec in) (parse-spec out))))

(defn do-cat [out-spec readers specs]
  (if specs
    (with-reader [rdr (first specs)]
      (recur out-spec (conj readers rdr) (next specs)))
    (let [headers (distinct (apply concat (map headers readers)))]
      (with-writer [wtr (assoc out-spec :headers headers)]
        (doseq [r readers]
          (reduce write-record wtr (record-seq r)))))))

(defn spec-or-default [spec]
  (if spec
    (parse-spec spec)
    (table-types ::tab)))

(defmethod main :cat [cmd & args]
  (with-command-line 
   args
   "cat - Concatenate some tables together.
Usage: anytable cat [options] <in-spec> [<in-spec>...]"
   [[out o "Write output to here."]
    specs]
   (let [dest (if out
                (parse-spec out)
                (table-types ::tab))]
     (do-cat dest [] (map parse-spec specs)))))

(defmethod main :cut [cmd & args]
  (with-command-line
      args
      "cut - Select some columns from the input table.
Usage: anytable cut [options] field [field...]"  
      [[in i "Read input from here."]
       [out o "Write output to here."]  
       cols]
    (with-reader [r (spec-or-default in)]
      (with-writer [w (assoc (spec-or-default out)
                        :headers cols)]
        (reduce write-row w (row-seq r))))))

(defmethod main :rename [cmd & args]
  (with-command-line
      args
      "rename - Rename some columns.
Usage: anytable rename [options] in1 out1 [in2 out2 ...]

Each pair of in* out* values will cause anytable to rename the column
identified by in* to out*."  
      [[in i "Read input from here."]
       [out o "Write output to here."]  fields]
    (with-reader [r (spec-or-default in)]
      (let [pairs        (partition 2 fields)
            replacements (zipmap (map first pairs) (map second pairs))
            cols         (replace replacements (headers r))]
        (with-writer [w (assoc (spec-or-default out)
                          :headers cols)]
          (reduce write-row w (row-seq r)))))))

(defmethod main :filter [cmd & args]
  (with-command-line
      args
      "filter - Select rows from an input table based on some criteria."  
      [[in i "Read input from here."]
       [out o "Write output to here."]  
       crit]
    (let [crit (eval (read-string (first crit)))]
      (println "Crit is" crit)
      (when-not (fn? crit)
        (throw (Exception. "criteria should evaluate to a function that takes a record")))
      (with-reader [r (spec-or-default in)]
        (with-writer [w (assoc (spec-or-default out)
                          :headers (headers r))]
          (doseq [rec (record-seq r)]
            (when (crit rec)
              (write-record w rec))))))))

(defmethod main :help 
  ([help]
     (doseq [method (keys (methods main))]
       (println method)))

  ([help & args]
     (let [arg  (first args)
           type (str-to-type arg)
           cmd  (keyword arg)]
       (cond
         (= "types" arg)
         (do
           (println "Table types are:")
           (doseq [[k v] @table-types]
             (println " " (.getName k) "-" (:doc ^v))))
         
         ((methods main) cmd)
         (println "Give help for 'anytable" arg "'")
         
         (table-types type)
         (let [default (table-types type)]
           (let [doc (:doc ^default)
                 key-doc (:key-doc ^default)]
             (println (:doc ^default))
             (println "This type supports the following keys:")
             (doseq [[k v] default :when (not (= k :type))]
               (if v
                 (printf "  %s - (%s) %s%n" k v (key-doc k))
                 (printf "  %s - %s %n" k (key-doc k)))
               (flush))))))))

(defn -main [& args]
  (if (empty? args)
    (main :help)
    (let [cmd (keyword (first args))
          args (next args)]
      (apply main cmd args))))

