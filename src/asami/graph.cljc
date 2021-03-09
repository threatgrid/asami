(ns ^{:doc "The graph index API."
      :author "Paula Gearon"}
    asami.graph
  (:require [schema.core :as s :include-macros true]
            [clojure.string :as string])
  #?(:clj (:import [java.io Writer])))


(defprotocol Graph
  (new-graph [this] "Creates an empty graph of the same type")
  (graph-add [this subj pred obj tx] "Adds triples to the graph")
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

#?(:clj
   (deftype InternalNode [^long id]
     Object
     (toString [_] (str "#a/n \"" id "\""))
     (equals [_ o] (and (instance? InternalNode o) (= id (.id o))))
     (hashCode [_] (hash id)))

   :cljs
   (deftype InternalNode [^long id]
     Object
     (toString [_] (str "#a/n \"" id "\""))

     IEquiv
     (-equiv [_ o] (and (instance? InternalNode o) (= id (.-id o))))

     IHash
     (-hash [_] (hash id))

     IPrintWithWriter
     (-pr-writer [this writer _] (-write writer (str this)))))

#?(:clj
   (defmethod clojure.core/print-method InternalNode [o ^Writer w]
     (.write w "#a/n \"")
     (.write w (str (.id o)))
     (.write w "\"")))

(defn node-read
  "Reads a node from a string"
  [s]
  (InternalNode.
   #?(:clj (Long/parseLong s)
      :cljs (long s))))

(def node-reader {'a/n node-read})

;; common implementations of the NodeAPI functions
(def tg-ns "tg")
(def node-prefix "node-")
(def prefix-len (count node-prefix))

(defn new-node
  ([] (->> node-prefix gensym name (keyword tg-ns)))
  ([id] (InternalNode. id)))

(defn node-id [n] (subs (name n) prefix-len))

(defn node-type? [n]
  (or 
   (instance? InternalNode n)
   (and (keyword? n) (= tg-ns (namespace n)) (string/starts-with? (name n) node-prefix))))

(defn node-label
  "Returns a keyword label for a node"
  [n]
  (keyword tg-ns (str "id-" (node-id n))))

