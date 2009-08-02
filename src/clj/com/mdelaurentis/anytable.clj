(ns com.mdelaurentis.anytable
  (:require [clojure.contrib.duck-streams :as streams])
  (:gen-class com.mdelaurentis.anytable.TableReader))

(declare open open? close headers row-seq record-seq)

(derive ::tab ::flat-file)
(derive ::fixed-width ::flat-file)

(defmulti open :type)
(defmulti make-row (fn [spec line]
                     (:type spec)))
(def headers :headers)
(defmulti row-seq :type)

(defn record-seq [table-spec]
  (let [hs (headers table-spec)]
    (map (partial zipmap hs) (row-seq table-spec))))

;; Delimited flat files

(defn tab-table [loc]
  {:type ::tab
   :location loc
   :delimiter "\t"})

(defmethod make-row ::tab [spec line]
  (.split line (:delimiter spec)))

(defmethod open ::tab [table-spec]
  (let [rdr     (streams/reader (:location table-spec))
        headers (make-row table-spec (first (line-seq rdr)))]
    (assoc table-spec
      :headers headers
      :reader  rdr)))

(defmulti close :type)

(defmethod close ::tab [t]
  (.close (:reader t)))

(defmethod close ::fixed-width [t]
  (.close (:reader t)))

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

(defmethod open ::fixed-width [spec]
  (let [rdr (streams/reader (:location spec))]
    (assoc spec
      :reader rdr)))

(defmethod make-row ::fixed-width [spec line]
  (for [[start end] (partition 2 1 (:bounds spec))]
    (.trim (.substring line start end))))

(defmethod row-seq ::flat-file [t]
  (map (partial make-row t) (line-seq (:reader t))))

(defmacro with-reader [[name spec] & body]
  `(let [~name (open ~spec)]
     (try ~@body
          (finally 
           (close ~name)))))