(ns ^{:doc "An graph wrapper for seqs"
      :author "Paula Gearon"}
    asami.seqgraph
  (:require [asami.graph :as gr :refer [Graph graph-add graph-delete graph-diff resolve-triple count-triple]]
            [asami.common-index :as common :refer [? NestedIndex]]
            [asami.analytics :as analytics]
            [zuko.node :refer [NodeAPI]]
            [zuko.logging :as log :include-macros true]
            [schema.core :as s :include-macros true]))

(defn subseq
  "A subvec wrapper that drops back to sequences when the seq is not a vector"
  [v a b]
  (if (vector? v)
    (subvec v a b)
    (drop a (take b v))))

(defmulti get-from-index
  "Lookup an index in the graph for the requested data.
   Returns a sequence of unlabelled bindings. Each binding is a vector of binding values."
  common/simplify)

(defmethod get-from-index [:v :v :v]
  [data s p o]
  (if (some (fn [[a b c]] (and (= s a) (= p b) (= o c))) data) [[]] []))

(defmethod get-from-index [:v :v  ?]
  [data s p o]
  (sequence
   (comp
    (filter (fn [[a b _]] (and (= s a) (= p b))))
    (map #(subseq % 2 3)))
   data))

(defmethod get-from-index [:v  ? :v]
  [data s p o]
  (sequence
   (comp
    (filter (fn [[a _ c]] (and (= s a) (= o c))))
    (map #(subseq % 1 2)))
   data))

(defmethod get-from-index [:v  ?  ?]
  [data s p o]
  (sequence
   (comp
    (filter (fn [[a _ _]] (= a s)))
    (map #(subseq % 1 3)))
   data))

(defmethod get-from-index [ ? :v :v]
  [data s p o]
  (sequence
   (comp
    (filter (fn [[_ b c]] (and (= p b) (= o c))))
    (map #(subseq % 0 1)))
   data))

(defmethod get-from-index [ ? :v  ?]
  [data s p o]
  (sequence
   (comp
    (filter (fn [[_ b _]] (= b p)))
    (map #(vector (first %) (nth % 2))))
   data))

(defmethod get-from-index [ ?  ? :v]
  [data s p o]
  (sequence
   (comp
    (filter (fn [[_ _ c]] (= c o)))
    (map #(subseq % 0 2)))
   data))

(defmethod get-from-index [ ?  ?  ?]
  [data s p o]
  (map #(subseq % 0 3) data))


(declare empty-graph)

(defrecord GraphSeq [data]
  Graph
  (new-graph [this] empty-graph)
  (graph-add [this subj pred obj]
    (graph-add this subj pred obj gr/*default-tx-id*))
  (graph-add [this subj pred obj tx]
    (log/trace "insert: " [subj pred obj tx])
    (update this :data conj [subj pred obj tx]))
  (graph-delete [this subj pred obj]
    (log/trace "delete " [subj pred obj])
    (update this :data #(remove (fn [[s p o]] (and (= subj s) (= pred p) (= obj o))) %)))
  (graph-transact [this tx-id assertions retractions]
    (throw (ex-info "Unsupported operation" {:operation :graph-transact})))
  (graph-transact [this tx-id assertions retractions generated-data]
    (throw (ex-info "Unsupported operation" {:operation :graph-transact})))
  (graph-diff [this other]
    (throw (ex-info "Unsupported operation" {:operation :graph-diff})))
  (resolve-triple [this subj pred obj]
    (if-let [[plain-pred trans-tag] (common/check-for-transitive pred)]
      (throw (ex-info "Unsupported operation" {:operation :transitive-resolve-triple}))
      (get-from-index data subj pred obj)))
  (count-triple [this subj pred obj]
    (count (resolve-triple this subj pred obj))))

(def empty-graph (->GraphSeq []))

(defn new-graph [data] (->GraphSeq data))
