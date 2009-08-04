(ns com.mdelaurentis.anytable.test
  (:import [java.io File])
  (:use [com.mdelaurentis anytable]
        [clojure.contrib test-is]))

(def breed-headers ["breed" "category" "size"])

(def expected-breeds
     [{"breed" "Beagle"       "category" "Hound" "size" "Medium" }
      {"breed" "Basset Hound" "category" "Hound" "size" "Large" }
      {"breed" "Chihuahua"    "category" ""      "size" "Small" }])

(deftest test-vectors
  (with-writer [wtr (vector-table :headers ["breed" "category" "size"])]
    (-> wtr
        (write-row ["Beagle"       "Hound" "Medium"])
        (write-row ["Basset Hound" "Hound" "Large"])
        (write-row ["Chihuahua"    ""      "Small"]))))

(deftest test-read-tab
  (is (= expected-breeds
         (with-reader [breeds (tab-table "/Users/mdelaurentis/src/anytable/test-input/breeds.tab")]
           (into [] (record-seq breeds))))))

(deftest test-read-fixed-width
  (is (= expected-breeds
         (with-reader [breeds (fixed-width-table "/Users/mdelaurentis/src/anytable/test-input/breeds.txt"
                                                 "breed"    12
                                                 "category"  5
                                                 "size"      6)]
           (doall (record-seq breeds))))))

(deftest test-write-tab
  (let [file (File/createTempFile "breeds" ".tab")]
    (with-writer 
     [breeds (tab-table file :headers breed-headers)]
     (doseq [breed expected-breeds]
       (write-record breeds breed)))
    (is (= expected-breeds
           (with-reader [breeds (tab-table file)]
             (doall (record-seq breeds)))))))

(run-tests)

(:rows (copy  (tab-table "/Users/mdelaurentis/src/anytable/test-input/breeds.tab") (vector-table)))