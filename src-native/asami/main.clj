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
  (:import [java.io PushbackReader]
           [java.nio CharBuffer])
  (:gen-class))

(set! *warn-on-reflection* true)

(def eof
  (Object.))

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

(gen-class
  :name "asami.PBR"
  :extends java.io.PushbackReader
  :prefix "pbr-"
  :init init
  :state qstate
  :constructors {[java.io.Reader clojure.lang.IDeref] [java.io.Reader]}
  :exposes-methods {read readSuper})

(defn pbr-init
  [reader query-acc]
  [[reader] {:dbl-newline (atom false)
             :query-acc query-acc}])

(defn pbr-read
  "Returns characters from the reader for an edn parser.
  When queries are not being accumulated, then pass through"
  [^asami.PBR this]
  (let [{:keys [dbl-newline query-acc]} (.qstate this)]
    (if-not @query-acc 
      (.readSuper this)  ;; not accumulating. Pass through.

      ;; check if this is returning 2 characters after a newline was pressed.
      (if @dbl-newline
        (do ;; The first character was already returned.
          (reset! dbl-newline false)
          (int \newline))
        (let [c (.readSuper this)]
          (case c   ;; intercept \; and \newline characters
            59 0    ;; \; character
            10 (do  ;; \newline character
                 (swap! dbl-newline not)  ;; remember that a second character will be needed
                 0)  ;; This is the first of 2 characters returned
            c))))))  ;; pass through by default

(defn derive-input
  "Convert command line options into the input stream required"
  [{:keys [interactive? query]}]
  (if (or (= query "-") interactive?)
    *in*
    (.getBytes ^String query)))

(defn separator?
  "Check for a magic 'empty' symbol that is used to indicate a query separator"
  [s]
  (= (symbol "\0") s))

(defn repl [input db prompt]
        ;; function to print a prompt after every newline
  (let [prompt-fn (if (some? prompt)
                    (fn [] (print prompt) (flush))
                    (constantly nil))
        ;; executes a parsed edn query and prints the result
        execute (fn [query]
                  (try
                    (pprint (asami/q query db))
                    (catch Exception e
                      (printf "Error executing query %s: %s\n" (pr-str query) (ex-message e)))))
        ;; maintain state to accumulate queries not wrapped in an edn structure
        query-acc (atom nil)
        ;; create the input reader, providing the query accumulation state
        stream (asami.PBR. (io/reader input) query-acc)]
    (loop []
      ;; prompt when necessary
      (when-not @query-acc (prompt-fn))
      ;; get the next
      (let [query (try (edn/read reader-opts stream) (catch Exception e e))]
        (cond
          (instance? Exception query)
          (do (printf "Error: %s\n" (ex-message query))
              (println "Type: " (type query))
              (.printStackTrace ^Exception query)
              nil)

          (eof? query)
          (when-let [q @query-acc]
            (execute q)
            nil)

          (separator? query) 
          (do (execute @query-acc)
              (reset! query-acc nil)
              (recur))

          :else
          (do
            (if @query-acc            ;; check if a query is being accumulated
              (swap! query-acc conj query) ;; add to the accumulated query

              ;; else, a complete query structure
              (if (or (sequential? query) (map? query))  ;; check if this a complete query
                (execute query)
                (reset! query-acc [query])))  ;; otherwise, start accumulating a new query
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
          input (derive-input options)
          prompt (when interactive?
                   (println "Unwrapped queries may be separated with ;")
                    "?- ")]
      (repl input db prompt))

    (System/exit 0)))
