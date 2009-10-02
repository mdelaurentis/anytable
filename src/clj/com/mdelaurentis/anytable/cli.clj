;; Anytable command-line tool

(ns com.mdelaurentis.anytable.cli
  (:import [java.io File]
           [java.util.regex Pattern]
           [java.net URI MalformedURLException URL])
  (:require [clojure.contrib.duck-streams :as streams])
  (:use [clojure.contrib sql command-line]
        [com.mdelaurentis anytable])
  (:gen-class))

(defmulti main (fn [cmd & args]
                 cmd))

(def commands (ref {}))

(defmacro defcmd [cmd desc cmdspec & body]
  (dosync
   (alter commands assoc cmd {:desc desc :cmdspec cmdspec})
   `(defmethod main ~cmd [_# & args#]
      (with-command-line args# ~desc ~cmdspec ~@body))))

(defcmd :cp
    "Copy a table from one place to another."
    [args]
  (when (not (= 2 (count args)))
    (throw (Exception. "Usage: anytable cp <in-spec> <out-spec>")))
  (let [[in out] args]
    (copy (anytable-spec in) (anytable-spec out))))


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
    (anytable-spec spec)
    (table-types ::tab)))

(defcmd :cat
   "Concatenate some tables together.
Usage: anytable cat [options] <in-spec> [<in-spec>...]"  
   [[out o "Write output to here."]
    specs]
   (let [dest (if out
                (anytable-spec out)
                (table-types ::tab))]
     (do-cat dest [] (map anytable-spec specs))))

(defcmd :cut
      "Select some columns from the input table.
Usage: anytable cut [options] field [field...]"
      [[in i "Read input from here."]
       [out o "Write output to here."]  
       cols] 
    (println "Cutting " cols "from" in)
    (with-reader [r (spec-or-default in)]
      (with-writer [w (assoc (spec-or-default out)
                        :headers cols)]
        (reduce write-record w (record-seq r)))))

(defcmd :rename
      "Rename some columns.
Usage: anytable rename [options] in1 out1 [in2 out2 ...]

Each pair of in* out* values will cause anytable to rename the column
identified by in* to out*." 
      [[in i "Read input from here."]
       [out o "Write output to here."]  fields]
      (println "Renaming stuff from" in "to" out ":" fields)
      (with-reader [r (spec-or-default in)]
        (let [pairs        (partition 2 fields)
              replacements (zipmap (map first pairs) (map second pairs))
              cols         (replace replacements (headers r))]
          (with-writer [w (assoc (spec-or-default out)
                            :headers cols)]
            (reduce write-row w (row-seq r))))))

(defcmd :filter 
  "Select rows from an input table based on some criteria."  
  [[in i "Read input from here."]
   [out o "Write output to here."]  
   crit]
  (let [crit (eval (read-string (first crit)))]
    (when-not (fn? crit)
      (throw (Exception. "criteria should evaluate to a function that takes a record")))
    (with-reader [r (spec-or-default in)]
      (with-writer [w (assoc (spec-or-default out)
                        :headers (headers r))]
        (doseq [rec (record-seq r)]
          (when (crit rec)
            (write-record w rec)))))))

(defn first-sentence [string]
  (second (re-matches (Pattern/compile "(.+?\\.).*" Pattern/DOTALL) string)))

(defmethod main :help 
  ([help]
     (println "Usage: anytable <command> [options]")
     (newline)
     (println "Commands:")
     (doseq [cmd (keys (methods main))]
       (println " " (.substring (str cmd) 1) "-" 
                (first-sentence (:desc (commands cmd) ""))))
     (newline)
     (println "Try \"anytable help <cmd>\" to get detailed help for a command."))

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
         (print-help (:desc (commands cmd))
                     (make-map [] (:cmdspec (commands cmd))))
         
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