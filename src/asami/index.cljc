(ns ^{:doc "An in-memory graph implementation with full indexing."
      :author "Paula Gearon"}
    asami.index
  (:require [naga.schema.store-structs :as st]
            #?(:clj  [schema.core :as s]
               :cljs [schema.core :as s :include-macros true])))

(def ? :?)

(s/defn index-add :- {s/Any {s/Any #{s/Any}}}
  "Add elements to a 3-level index"
  [idx :- {s/Any {s/Any s/Any}}
   a :- s/Any
   b :- s/Any
   c :- s/Any]
  (update-in idx [a b] (fn [v] (if (seq v) (conj v c) #{c}))))

(s/defn index-delete :- {s/Any {s/Any #{s/Any}}}
  "Remove elements from a 3-level index. Returns the new index, or nil if there is no change."
  [idx :- {s/Any {s/Any #{s/Any}}}
   a :- s/Any
   b :- s/Any
   c :- s/Any]
  (if-let [idx2 (idx a)]
    (if-let [idx3 (idx2 b)]
      (let [new-idx3 (disj idx3 c)]
        (if-not (identical? new-idx3 idx3)
          (let [new-idx2 (if (seq new-idx3) (assoc idx2 b new-idx3) (dissoc idx2 b))
                new-idx (if (seq new-idx2) (assoc idx a new-idx2) (dissoc idx a))]
            new-idx))))))

(defn- simplify [g & ks] (map #(if (st/vartest? %) ? :v) ks))

(defmulti get-from-index "Lookup an index in the graph for the requested data" simplify)

(defmethod get-from-index [:v :v :v] [{idx :spo} s p o] (let [os (get-in idx [s p])] (if (get os o) [[]] [])))
(defmethod get-from-index [:v :v  ?] [{idx :spo} s p o] (map vector (get-in idx [s p])))
(defmethod get-from-index [:v  ? :v] [{idx :osp} s p o] (map vector (get-in idx [o s])))
(defmethod get-from-index [:v  ?  ?] [{idx :spo} s p o] (let [edx (idx s)] (for [p (keys edx) o (edx p)] [p o])))
(defmethod get-from-index [ ? :v :v] [{idx :pos} s p o] (map vector (get-in idx [p o])))
(defmethod get-from-index [ ? :v  ?] [{idx :pos} s p o] (let [edx (idx p)] (for [o (keys edx) s (edx o)] [s o])))
(defmethod get-from-index [ ?  ? :v] [{idx :osp} s p o] (let [edx (idx o)] (for [s (keys edx) p (edx s)] [s p])))
(defmethod get-from-index [ ?  ?  ?] [{idx :spo} s p o] (for [s (keys idx) p (keys (idx s)) o ((idx s) p)] [s p o]))

(defprotocol Graph
  (graph-add [this subj pred obj] "Adds triples to the graph")
  (graph-delete [this subj pred obj] "Removes triples from the graph")
  (graph-diff [this other] "Returns all subjects that have changed in this graph, compared to other")
  (resolve-triple [this subj pred obj] "Resolves patterns from the graph, and returns unbound columns only"))

(defrecord GraphIndexed [spo pos osp]
  Graph
  (graph-add [this subj pred obj]
    (let [new-spo (index-add spo subj pred obj)]
      (if (identical? spo new-spo)
        this
        (assoc this :spo new-spo
               :pos (index-add pos pred obj subj)
               :osp (index-add osp obj subj pred)))))
  (graph-delete [this subj pred obj]
    (if-let [idx (index-delete spo subj pred obj)]
      (assoc this :spo idx :pos (index-delete pos pred obj subj) :osp (index-delete osp obj subj pred))
      this))
  (graph-diff [this other]
    (let [s-po (remove (fn [[s po]] (= po (get (:spo other) s)))
                       spo)]
      (map first s-po)))
  (resolve-triple [this subj pred obj]
    (get-from-index this subj pred obj)))

(defn resolve-pattern
  "Convenience function to extract elements out of a pattern to query for it"
  [graph [s p o :as pattern]]
  (resolve-triple graph s p o))

(def empty-graph (->GraphIndexed {} {} {}))
