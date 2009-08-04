(ns com.mdelaurentis.anytable
  (:require [clojure.contrib.duck-streams :as streams])
  (:gen-class com.mdelaurentis.anytable.TableReader))

(declare open? close headers row-seq record-seq)

(derive ::tab ::flat-file)
(derive ::fixed-width ::flat-file)

(defmulti open-reader :type)

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

(def headers :headers)

(defn record-seq [table-spec]
  (let [hs (headers table-spec)]
    (map (partial zipmap hs) (row-seq table-spec))))

(defn write-record [t record]
  (write-row t (map record (headers t))))

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


;; In-memory vector of vectors

(defn vector-table [& options] 
  (merge {:type ::vectors
          :rows []} 
         (apply hash-map options)))

(defmethod open-reader ::vectors [t] t)
(defmethod open-writer ::vectors [t] 
  (write-row t (:headers t)))

(defmethod row-seq ::vectors [t] (:rows t))
(defmethod write-row ::vectors [t row]
  (assoc t
    :rows (conj (:rows t) row)))

(defmethod close ::vectors [t])

;; Flat text files

(defmulti parse-row (fn [spec line]
                     (:type spec)))
(defmulti format-row (fn [spec row]
                       (:type spec)))

(defmethod row-seq ::flat-file [t]
  (map (partial parse-row t) (line-seq (:reader t))))

(defmethod write-row ::flat-file [t row]
  (binding [*out* (:writer t)]
    (println (format-row t row))))

(defmethod close ::flat-file [t]
  (when-let [r (:reader t)] (.close r))
  (when-let [w (:writer t)] (.close w)))

;; Delimited flat files

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

;; Fixed-width flat files

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

(defn copy [in out]
  (with-reader [rdr in]
    (with-writer [wtr (assoc out :headers (:headers in))]
      (reduce write-row wtr (row-seq rdr)))))