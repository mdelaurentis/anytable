;; Implementation of Anytable interface

(ns com.mdelaurentis.anytable.AnytableImpl
  (:use [com.mdelaurentis.anytable])
  (:gen-class
   :implements [com.mdelaurentis.anytable.Anytable]
   :constructors {[Object] []}
   :init init
   :state state))

(defn -init [spec]
  [[] (anytable-spec spec)])

(defn -headers [this]
  (headers (.state this)))

(defn -headers [this]
  (headers (.state this)))

(defn -spec [this]
  (.state this))

(defn -openReader [this]
  (com.mdelaurentis.anytable.AnytableImpl. (open-reader (.state this))))

(defn -openWriter [this]
  (com.mdelaurentis.anytable.AnytableImpl. (open-writer (.state this))))

(defn -rows [this]
  (row-seq (.state this)))

(defn -records [this]
  (record-seq (.state this)))

(defn -writeRow [this row]
  (write-row (.state this)))

(defn -writeRecord [this rec]
  (write-record (.state this)))

(defn -close [this]
  (close (.state this)))