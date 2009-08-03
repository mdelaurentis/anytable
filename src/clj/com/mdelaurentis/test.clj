(ns com.mdelaurentis.anytable.test
  (:import [java.io File])
  (:use [com.mdelaurentis anytable]
        [clojure.contrib test-is]))

(def expected-breeds
     [{"breed" "Beagle"       "category" "Hound" "size" "Medium" }
      {"breed" "Basset Hound" "category" "Hound" "size" "Large" }
      {"breed" "Chihuahua"    "category" ""      "size" "Small" }])

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
    ))

(run-tests)