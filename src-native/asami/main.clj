(ns ^{:doc "Entry point for CLI"
      :author "Paula Gearon"}
  asami.main
  (:require [asami.core :as asami]
            [asami.graph :as graph]
            [cheshire.core :as json]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]
            [clojure.string :as s])
  (:import (java.io PushbackReader))
  (:gen-class))

(def eof
  (reify))

(defn eof? [x]
  (identical? x eof))

(def reader-opts
  {:eof eof
   :readers (assoc graph/node-reader 'a/r re-pattern)})

(defn process-args
  [args]
  (loop [result {:interactive? false
                 :query "-"}
         [a & [arg & rargs :as rem] :as args] args]
    (if-not (seq args)
      result
      (let [[r more] (cond
                       (#{"-e" "-q"} a) [(assoc result :query arg) rargs]
                       (= a "-f") [(assoc result :file arg) rargs]
                       (#{"-?" "--help"} a) [(assoc result :help true) rem]
                       (= a "--interactive") [(assoc result :interactive? true) rem]
                       (s/starts-with? a "asami:") [(assoc result :url a) rem])]
        (recur r more)))))

(defn read-data-file
  [f]
  (if (s/ends-with? f ".json")
    (json/parse-string (slurp f))
    (let [text (slurp (if (= f "-") *in* f))]
      (edn/read-string reader-opts text))))

(defn load-data-file
  [conn f]
  (:db-after @(asami/transact conn {:tx-data (read-data-file f)})))

(defn derive-database [{:keys [file url]}]
  (let [conn (asami/connect url)]
    (if file
      (load-data-file conn file)
      (asami/db conn))))

(defn derive-stream [{:keys [interactive? query]}]
  (let [input (if (or (= query "-") interactive?)
                *in*
                (.getBytes ^String query))]
    (PushbackReader. (io/reader input))))

(defn repl [stream db prompt]
  (let [prompt-fn (if (some? prompt)
                    (fn [] (print prompt) (flush))
                    (constantly nil))]
    (loop []
      (prompt-fn)
      (let [query (try (edn/read reader-opts stream) (catch Exception e e))]
        (cond
          (instance? Exception query)
          (do (printf "Error: %s\n" (ex-message query))
              (recur))

          (eof? query)
          nil

          :else
          (do (try
                (pprint (asami/q query db))
                (catch Exception e
                  (printf "Error executing query %s: %s\n" (pr-str query) (ex-message e))))
              (recur)))))))

(defn print-usage
  []
  (println "Usage: asami URL [-f filename] [-e query] [--help | -?] [--interactive]\n")
  (println)
  (println "URL: The URL of the database to use. Must start with asami:mem://, asami:multi:// or asami:local://")
  (println)
  (println "Options:")
  (println "  -?,  --help  This help")
  (println)
  (println "  -f FILENAME  Loads the file FILENAME into the database. A FILENAME of \"-\" will use STDIN.")
  (println "               Data defaults to EDN. A FILENAME ending in .json is treated as JSON.")
  (println "  -e QUERIES   Executes queries in the string QUERIES. QUERIES are specified as EDN and, thus, multiple queries may be separated with the usual EDN whitespace characters.")
  (println "               When this option is not provided, queries will read from STDIN instead of a command line argument.")
  (println)
  (println "Available EDN readers:")
  (println "  internal nodes -  #a/n \"node-id\"")
  (println "  regex          -  #a/r \"[Tt]his is a (regex|regular expression)\""))

(defn -main
  [& args]
  (let [{:keys [help interactive? url] :as options} (process-args args)]
    (when help
      (print-usage)
      (System/exit 0))

    (when-not url
      (println "Database URL must be specified")
      (System/exit 1))

    (let [db (derive-database options)
          stream (derive-stream options)
          prompt (if interactive? "?- ")]
      (repl stream db prompt))

    (System/exit 0)))
