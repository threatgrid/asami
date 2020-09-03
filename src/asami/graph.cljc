(ns ^{:doc "The graph index API."
      :author "Paula Gearon"}
    asami.graph
  (:require #?(:clj  [schema.core :as s]
               :cljs [schema.core :as s :include-macros true])
            [clojure.string :as string]))


(defprotocol Graph
  (new-graph [this] "Creates an empty graph of the same type")
  (graph-add [this subj pred obj] "Adds triples to the graph")
  (graph-delete [this subj pred obj] "Removes triples from the graph")
  (graph-transact [this tx-id assertions retractions] "Bulk operation to add and remove multiple statements in a single operation")
  (graph-diff [this other] "Returns all subjects that have changed in this graph, compared to other")
  (resolve-triple [this subj pred obj] "Resolves patterns from the graph, and returns unbound columns only")
  (count-triple [this subj pred obj] "Resolves patterns from the graph, and returns the size of the resolution"))

(def GraphType (s/pred #(satisfies? Graph %1)))

(defn resolve-pattern
  "Convenience function to extract elements out of a pattern to query for it"
  [graph [s p o :as pattern]]
  (resolve-triple graph s p o))

(defn count-pattern
  "Convenience function to extract elements out of a pattern to count the resolution"
  [graph [s p o :as pattern]]
  (count-triple graph s p o))

(def tg-ns "tg")
(def node-prefix "node-")
(def prefix-len (count node-prefix))

;; common implementations of the NodeAPI functions
(defn new-node [] (->> node-prefix gensym name (keyword tg-ns)))

(defn node-id [n] (subs (name n) prefix-len))

(defn node-type? [n] (and (keyword? n) (= tg-ns (namespace n)) (string/starts-with? (name n) node-prefix)))

(defn node-label
  "Returns a keyword label for a node"
  [n]
  (keyword tg-ns (str "id-" (node-id n))))
