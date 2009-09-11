(ns com.mdelaurentis.anytable.test
  (:import [java.io File]
           [java.net URL]
           [org.hsqldb.jdbc JDBCDataSource])
  
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
         (record-seq (copy (tab-table "sample-data/breeds.tab")
                           (vector-table))))))

(deftest test-read-fixed-width
  (is (= (record-seq breeds)
         (record-seq (copy  (fixed-width-table "sample-data/breeds.txt"
                                               "breed"    12
                                               "category"  5
                                               "size"      6)
                            (vector-table))))))

(deftest test-write-tab
  (let [file (File/createTempFile "breeds" ".tab")]
    (copy breeds (tab-table file :headers breed-headers))
    (is (= (record-seq breeds)
           (record-seq (copy (tab-table file) (vector-table)))))))

(deftest test-jdbc-insert-sql 
  (let [spec (hsqldb-table :table-name "breeds"
                           :column-specs [[:breed "VARCHAR(32)"]
                                          [:category "VARCHAR(32)"]
                                          [:size "VARCHAR(32)"]])]
    (is (= "INSERT INTO breeds VALUES(?, ?, ?)"
           (insert-sql spec)))))

(deftest test-jdbc
  (let [spec (hsqldb-table :table-name "breeds"
                           :column-specs [[:breed "VARCHAR(32)"]
                                          [:category "VARCHAR(32)"]
                                          [:size "VARCHAR(32)"]])]
    (copy breeds spec)
    (with-reader [rdr spec]
      (is (= (row-seq breeds)
             (row-seq rdr))))))

(deftest test-anytable-spec
  (let [expected (tab-table (File. "foo.tab") :delimiter "\t")]
    (is (= (anytable-spec expected)
           expected))
    (is (= (anytable-spec (File. "foo.tab"))
           expected))
    (is (= (anytable-spec "foo.tab")
           (tab-table (File. "foo.tab") :delimiter "\t")))
    (is (= (anytable-spec "file:///foo.tab")
           (tab-table (URL. "file:///foo.tab") :delimiter "\t")))))

(run-tests)

