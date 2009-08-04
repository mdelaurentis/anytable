(ns com.mdelaurentis.anytable.test
  (:import [java.io File])
  (:use [com.mdelaurentis anytable]
        [clojure.contrib test-is]))

(def breed-headers ["breed" "category" "size"])



(def breeds
  (reduce write-row
          (vector-table 
           :headers
           ["breed" "category" "size"])
          [["Beagle"       "Hound" "Medium"]
           ["Basset Hound" "Hound" "Large"]
           ["Chihuahua"    ""      "Small"]]))


(def expected-breeds
     [{"breed" "Beagle"       "category" "Hound" "size" "Medium" }
      {"breed" "Basset Hound" "category" "Hound" "size" "Large" }
      {"breed" "Chihuahua"    "category" ""      "size" "Small" }])

(deftest test-read-tab
  (is (= (record-seq breeds)
         (record-seq (copy (tab-table "/Users/mdelaurentis/src/anytable/test-input/breeds.tab")
                           (vector-table))))))

(deftest test-read-fixed-width
  (is (= (record-seq breeds)
         (record-seq (copy  (fixed-width-table "/Users/mdelaurentis/src/anytable/test-input/breeds.txt"
                                               "breed"    12
                                               "category"  5
                                               "size"      6)
                            (vector-table))))))

(deftest test-write-tab
  (let [file (File/createTempFile "breeds" ".tab")]
    (copy breeds (tab-table file :headers breed-headers))
    (is (= (record-seq breeds)
           (record-seq (copy (tab-table file) (vector-table)))))))

(run-tests)

