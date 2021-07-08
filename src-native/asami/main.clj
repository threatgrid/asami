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

(def reader-opts
  {:eof (reify)
   :readers (assoc graph/node-reader 'a/r re-pattern)})

(def default-opts
  {:interactive? false
   :query "-"})

(defn process-args
  [args]
  (loop [opts default-opts
         [a & [arg & rargs :as rem]] args]
    (if-not (seq args)
      opts
      (let [[r more] (case a
                       ("-e" "-q") [(assoc opts :query arg) rargs]
                       "-f" [(assoc opts :file arg) rargs]
                       ("-?" "--help") [(assoc opts :help true) rem]
                       ("--interactive") [(assoc opts :interactive? true) rem]
                       ;; else
                       (if (s/starts-with? a "asami:")
                         [(assoc opts :url a) rem]))]
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

(def ^:const query-separator
  (int \;))

(defn read-queries [^String q]
  (let [ipt (if (= q "-") *in* (.getBytes q))
        pbr (PushbackReader. (io/reader ipt))
        eof (:eof reader-opts)]
    (loop [queries []]
      (let [c (.read pbr)]
        (cond
          (or (Character/isWhitespace c)
              (= c query-separator))
          (recur queries)

          (= c -1)
          queries

          :else
          (let [query (edn/read reader-opts (doto pbr (.unread c)))]
            (cond
              (identical? query eof)
              queries

              (keyword? query)
              (recur (conj queries [query]))

              ;; Many queries
              (and (vector? query) (every? vector? query))
              (recur (into queries query))

              ;; One query
              :else
              (recur (conj queries query)))))))))

(defn print-usage
  []
  (println "Usage: asami URL [-f filename] [-e query] [--help | -?]\n")
  (println "-? | --help: This help")
  (println "URL: the URL of the database to use. Must start with asami:mem://, asami:multi:// or asami:local://")
  (println "-f filename: loads the filename into the database. A filename of \"-\" will use stdin.")
  (println "             Data defaults to EDN. Filenames ending in .json are treated as JSON.")
  (println "-e query: executes a query. \"-\" (the default) will read from stdin instead of a command line argument.")
  (println "          Multiple queries can be specified as edn (vector of query vectors) or ; separated.")
  (println)
  (println "Available EDN readers:")
  (println "  internal nodes -  #a/n \"node-id\"")
  (println "  regex          -  #a/r \"[Tt]his is a (regex|regular expression)\""))

(defn derive-database [{:keys [file url]}]
  (let [conn (asami/connect url)]
    (if file
      (load-data-file conn file)
      (asami/db conn))))

(defn execute-queries [{:keys [query] :as options}]
  (let [db (derive-database options)]
    (doseq [query (read-queries query)]
      (pprint (asami/q query db)))))

(defn interactive [options]
  (let [db (derive-database options)
        pbr (PushbackReader. (io/reader *in*))
        eof (:eof reader-opts)]
    (loop []
      (print "?- ")
      (flush)
      (let [query (edn/read reader-opts pbr)]
        (when-not (identical? query eof)
          (pprint (asami/q query db))
          (recur))))))

(defn -main
  [& args]
  (let [{:keys [help interactive? url] :as options} (process-args args)]
    (when help
      (print-usage)
      (System/exit 0))

    (when-not url
      (println "Database URL must be specified")
      (System/exit 1))

    (if interactive?
      (interactive options)
      (execute-queries options))

    (System/exit 0)))
