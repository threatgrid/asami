(ns ^{:doc "Entry point for CLI"
      :author "Paula Gearon"}
  asami.main
  (:require [clojure.string :as s]
            [clojure.edn :as edn]
            [cheshire.core :as json]
            [clojure.pprint :refer [pprint]]
            [asami.core :as asami]
            [asami.graph :as graph])
  (:gen-class))

(def reader-opts {:readers (assoc graph/node-reader 'a/r re-pattern)})

(defn process-args
  [args]
  (loop [result {:query "-"} [a & [arg & rargs :as rem] :as args] args]
    (if-not (seq args)
      result
      (let [[r more] (cond
                       (#{"-e" "-q"} a) [(assoc result :query arg) rargs]
                       (= a "-f") [(assoc result :file arg) rargs]
                       (#{"-?" "--help"} a) [(assoc result :help true) rem]
                       (s/starts-with? a "asami:") [(assoc result :url a) rem])]
        (recur r more)))))

(defn read-data-file
  [f]
  (let [text (slurp (if (= f "-") *in* f))]
    (if (s/ends-with? f ".json")
      (json/parse-string text)
      (edn/read-string reader-opts text))))

(defn load-data-file
  [conn f]
  (:db-after @(asami/transact conn {:tx-data (read-data-file f)})))

(defn read-queries
  [q]
  (let [text (s/trim (if (= q "-") (slurp *in*) q))
        mtext (s/split text #";")
        all (mapv (fn [text]
                    (let [txt (if (= \: (first (s/trim text))) (str "[" text "]") text)]
                      (edn/read-string reader-opts txt)))
                  mtext)]
    (if (= 1 (count all))
      (let [[qv] all]
        (if (vector? (first qv)) qv all))
      all)))

(defn print-usage
  []
  (println "Usage: asami URL [-f filename] [-e query] [--help | -?]\n")
  (println "URL: the URL of the database to use. Must start with asami:mem://, asami:multi:// or asami:local://")
  (println "-f filename: loads the filename into the database. A filename of \"-\" will use stdin.")
  (println "             Data defaults to EDN. Filenames ending in .json are treated as JSON.")
  (println "-e query: executes a query. \"-\" (the default) will read from stdin instead of a command line argument.")
  (println "          Multiple queries can be specified as edn (vector of query vectors) or ; separated.")
  (println "          Available readers: internal nodes -  #a/n \"node-id\"")
  (println "                             regex          -  #a/r \"[Tt]his is a (regex|regular expression)\"")
  (println "-? | --help: This help"))

(defn -main
  [& args]
  (let [{:keys [query file url help]} (process-args args)]
    (when help
      (print-usage)
      (System/exit 0))
    (when-not url
      (println "Database URL must be specified")
      (System/exit 1))
    (let [conn (asami/connect url)
          db (if file
               (load-data-file conn file)
               (asami/db conn))]
      (doseq [query (read-queries query)]
        (pprint (asami/q query db)))
      (System/exit 0))))
