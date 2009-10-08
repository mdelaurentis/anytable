(ns com.mdelaurentis.anytable.doc
  (:use [compojure html]
        [com.mdelaurentis anytable])
  (:gen-class))

(defn doc-type [type-key]
  (let [type (table-types type-key)]
    (html [:div
           [:h3 (.getName type-key)]]
          (:doc ^type)
          [:table
           [:tr 
            [:th "parameter"]
            [:th "default"]
            [:th "description"]]
           (for [[key val] (:key-doc ^type)]
             [:tr 
              [:td key]
              [:td (type key)]
              [:td val]])])))

(defn doc-types []
  (html
   [:body
    [:h1 "Anytable Types"]
    (for [type-key (keys @table-types)]
      (doc-type type-key))]))

(defn -main []
  (println (doc-types)))