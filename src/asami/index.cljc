(ns ^{:doc "An in-memory graph implementation with full indexing."
      :author "Paula Gearon"}
    asami.index
  (:require [asami.graph :as gr :refer [Graph graph-add graph-delete graph-diff resolve-triple count-triple]]
            [asami.common-index :as common :refer [? NestedIndex]]
            [asami.analytics :as analytics]
            [zuko.node :refer [NodeAPI]]
            [zuko.logging :as log :include-macros true]
            [schema.core :as s :include-macros true]))

(def Index {s/Any {s/Any {s/Any {(s/required-key :t) s/Int ;transaction id
                                 (s/required-key :id) s/Int}}}}) ;statement id

(s/defn index-add :- Index
  "Add elements to a 4-level index.
   If triple already exists, returns given index unchanged."
  [idx :- Index
   a :- s/Any
   b :- s/Any
   c :- s/Any
   id :- s/Int
   t :- s/Int]
  (if-let [idxb (get idx a)]
    (if-let [idxc (get idxb b)]
      (if (get idxc c)
        idx
        (assoc idx a (assoc idxb b (assoc idxc c {:t t :id id}))))
      (assoc idx a (assoc idxb b {c {:t t :id id}})))
    (assoc idx a {b {c {:t t :id id}}})))

(s/defn index-delete :- (s/maybe Index)
  "Remove elements from a 4-level index. Returns the new index, or nil if there is no change."
  [idx :- Index 
   a :- s/Any
   b :- s/Any
   c :- s/Any]
  (if-let [idx2 (idx a)]
    (if-let [idx3 (idx2 b)]
      (let [new-idx3 (dissoc idx3 c)]
        (if-not (identical? new-idx3 idx3)
          (let [new-idx2 (if (seq new-idx3) (assoc idx2 b new-idx3) (dissoc idx2 b))
                new-idx (if (seq new-idx2) (assoc idx a new-idx2) (dissoc idx a))]
            new-idx))))))

(defmulti get-from-index
  "Lookup an index in the graph for the requested data.
   Returns a sequence of unlabelled bindings. Each binding is a vector of binding values."
  common/simplify)

;; Extracts the required index (idx), and looks up the requested fields.
;; If an embedded index is pulled out, then this is referred to as edx.
(defmethod get-from-index [:v :v :v] [{idx :spo} s p o] (if (some-> idx (get s) (get p) (get o) keys) [[]] []))
(defmethod get-from-index [:v :v  ?] [{idx :spo} s p o] (map vector (some-> idx (get s) (get p) keys)))
(defmethod get-from-index [:v  ? :v] [{idx :osp} s p o] (map vector (some-> idx (get o) (get s) keys)))
(defmethod get-from-index [:v  ?  ?] [{idx :spo} s p o] (let [edx (idx s)] (for [p (keys edx) o ((comp keys edx) p)] [p o])))
(defmethod get-from-index [ ? :v :v] [{idx :pos} s p o] (map vector (some-> idx (get p) (get o) keys)))
(defmethod get-from-index [ ? :v  ?] [{idx :pos} s p o] (let [edx (idx p)] (for [o (keys edx) s ((comp keys edx) o)] [s o])))
(defmethod get-from-index [ ?  ? :v] [{idx :osp} s p o] (let [edx (idx o)] (for [s (keys edx) p ((comp keys edx) s)] [s p])))
(defmethod get-from-index [ ?  ?  ?] [{idx :spo} s p o] (for [s (keys idx) p (keys (idx s)) o (keys ((idx s) p))] [s p o]))



(defmulti count-transitive-from-index
  "Lookup an index in the graph for the requested data and count the results based on a transitive index."
  common/trans-simplify)

;; TODO count these efficiently
(defmethod count-transitive-from-index :default
  [graph tag s p o]
  (if (= [? ? ?] (common/simplify graph s p o))
    ;; There is no sense in this except for planning, so estimate an upper bound
    (* (count (:spo graph)) (count (:osp graph)))
    (count (common/get-transitive-from-index graph tag s p o))))

(declare empty-graph)

(defrecord GraphIndexed [spo pos osp next-stmt-id]
  NestedIndex
  (lowest-level-fn [this] keys)
  (lowest-level-sets-fn [this] (partial map (comp set keys)))
  (lowest-level-set-fn [this] (comp set keys))
  (mid-level-map-fn [this]  #(into {} (map (fn [[k v]] [k (set (keys v))]) %)))

  Graph
  (new-graph [this] empty-graph)
  (graph-add [this subj pred obj]
    (graph-add this subj pred obj gr/*default-tx-id*))
  (graph-add [this subj pred obj tx]
    (log/trace "insert: " [subj pred obj tx])
    (let [id (or (:next-stmt-id this) 1)
          new-spo (index-add spo subj pred obj id tx)]
      (if (identical? spo new-spo)
        (do
          (log/trace "statement already existed")
          this)
        (assoc this :spo new-spo
               :pos (index-add pos pred obj subj id tx)
               :osp (index-add osp obj subj pred id tx)
               :next-stmt-id (inc id)))))
  (graph-delete [this subj pred obj]
    (log/trace "delete " [subj pred obj])
    (if-let [idx (index-delete spo subj pred obj)]
      (assoc this
             :spo idx
             :pos (index-delete pos pred obj subj)
             :osp (index-delete osp obj subj pred))
      (do
        (log/trace "statement did not exist")
        this)))
  (graph-transact [this tx-id assertions retractions]
    (as-> this graph
      (reduce (fn [acc [s p o]] (graph-delete acc s p o)) graph retractions)
      (reduce (fn [acc [s p o]] (graph-add acc s p o tx-id)) graph assertions)))
  (graph-diff [this other]
    (when-not (= (type this) (type other))
      (throw (ex-info "Unable to compare diffs between graphs of different types" {:this this :other other})))
    (let [s-po (remove (fn [[s po]] (= po (get (:spo other) s))) spo)]
      (map first s-po)))
  (resolve-triple [this subj pred obj]
    (if-let [[plain-pred trans-tag] (common/check-for-transitive pred)]
      (common/get-transitive-from-index this trans-tag subj plain-pred obj)
      (get-from-index this subj pred obj)))
  (count-triple [this subj pred obj]
    (if-let [[plain-pred trans-tag] (common/check-for-transitive pred)]
      (count-transitive-from-index this trans-tag subj plain-pred obj)
      (common/count-from-index this subj pred obj)))
  
  NodeAPI
  (data-attribute [_ _] :tg/first)
  (container-attribute [_ _] :tg/contains)
  (new-node [_] (gr/new-node))
  (node-id [_ n] (gr/node-id n))
  (node-type? [_ _ n] (gr/node-type? n))
  (find-triple [this [e a v]] (resolve-triple this e a v)))

(def empty-graph (->GraphIndexed {} {} {} nil))
