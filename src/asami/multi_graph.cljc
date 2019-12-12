(ns ^{:doc "A multi-graph implementation.
Resolution counting ignores multiple edges connecting nodes, so as to
allow rules to successfully use this graph type."
      :author "Paula Gearon"}
    asami.multi-graph
  (:require [asami.graph :refer [Graph new-graph graph-add graph-delete graph-diff
                                 resolve-triple count-triple]]
            [asami.common-index :as common :refer [? NestedIndex]]
            [asami.analytics :as analytics]
            #?(:clj  [schema.core :as s]
               :cljs [schema.core :as s :include-macros true])))

(def ^:dynamic *insert-op* #(if (zero? %) 1 %))

(def IndexStructure
  {s/Any
   {s/Any
    {s/Any
     {(s/required-key :count) s/Num
      s/Keyword s/Any}}}})

(s/defn multi-add :- IndexStructure
  "Add elements to a 4-level index"
  [idx :- IndexStructure
   a :- s/Any
   b :- s/Any
   c :- s/Any]
  (update-in idx [a b c :count] (fnil *insert-op* 0)))

(s/defn multi-delete :- IndexStructure
  "Remove elements from a 3-level index. Returns the new index, or nil if there is no change."
  [idx :- IndexStructure
   a :- s/Any
   b :- s/Any
   c :- s/Any]
  (if-let [idx2 (idx a)]
    (if-let [idx3 (idx2 b)]
      (if-let [c4 (:count (idx3 c))]
        (if (= 1 c4)
          (let [new-idx3 (dissoc idx3 c)
                new-idx2 (if (seq new-idx3) (assoc idx2 b new-idx3) (dissoc idx2 b))
                new-idx (if (seq new-idx2) (assoc idx a new-idx2) (dissoc idx a))]
            new-idx)
          (update-in idx [a b c :count] dec))))))

(defmulti get-from-multi-index
  "Lookup an index in the graph for the requested data.
   Returns a sequence of unlabelled bindings. Each binding is a vector of binding values."
  common/simplify)

;; Extracts the required index (idx), and looks up the requested fields.
;; If an embedded index is pulled out, then this is referred to as edx.
(defmethod get-from-multi-index [:v :v :v] [{idx :spo} s p o] (let [n (get-in idx [s p o :count])]
                                                                (if (and (number? n) (> n 0))
                                                                  (repeat n [])
                                                                  [])))
(defmethod get-from-multi-index [:v :v  ?] [{idx :spo} s p o] (for [[o {c :count}] (get-in idx [s p])
                                                                    _ (range c)]
                                                                [o]))
(defmethod get-from-multi-index [:v  ? :v] [{idx :osp} s p o] (for [[p {c :count}] (get-in idx [o s])
                                                                    _ (range c)]
                                                                [p]))
(defmethod get-from-multi-index [:v  ?  ?] [{idx :spo} s p o] (let [edx (idx s)]
                                                                (for [p (keys edx)
                                                                      [o {c :count}] (edx p)
                                                                      _ (range c)]
                                                                  [p o])))
(defmethod get-from-multi-index [ ? :v :v] [{idx :pos} s p o] (for [[s {c :count}] (get-in idx [p o])
                                                                    _ (range c)]
                                                                [s]))
(defmethod get-from-multi-index [ ? :v  ?] [{idx :pos} s p o] (let [edx (idx p)]
                                                                (for [o (keys edx)
                                                                      [s {c :count}] (edx o)
                                                                      _ (range c)]
                                                                  [s o])))
(defmethod get-from-multi-index [ ?  ? :v] [{idx :osp} s p o] (let [edx (idx o)]
                                                                (for [s (keys edx)
                                                                      [p {c :count}] (edx s)
                                                                      _ (range c)]
                                                                  [s p])))
(defmethod get-from-multi-index [ ?  ?  ?] [{idx :spo} s p o] (for [s (keys idx)
                                                                    p (keys (idx s))
                                                                    [o {c :count}] ((idx s) p)
                                                                    _ (range c)]
                                                                [s p o]))

(defmulti count-transitive-from-index
  "Lookup an index in the graph for the requested data and count the results based on a transitive index."
  common/trans-simplify)

;; TODO count these efficiently
(defmethod count-transitive-from-index :default
  [graph tag s p o]
  (count (common/get-transitive-from-index graph tag s p o)))

(declare empty-multi-graph)

(defrecord MultiGraph [spo pos osp]
  NestedIndex
  (lowest-level-fn [this] keys)
  (lowest-level-set-fn [this] (comp set keys))
  (lowest-level-sets-fn [this] (partial map (comp set keys)))
  (mid-level-map-fn [this] #(into {} (map (fn [[k v]] [k (set (keys v))]) %1)))

  Graph
  (new-graph [this] empty-multi-graph)
  (graph-add [this subj pred obj]
    (assoc this
           :spo (multi-add spo subj pred obj)
           :pos (multi-add pos pred obj subj)
           :osp (multi-add osp obj subj pred)))
  (graph-delete [this subj pred obj]
    (if-let [idx (multi-delete spo subj pred obj)]
      (assoc this :spo idx :pos (multi-delete pos pred obj subj) :osp (multi-delete osp obj subj pred))
      this))
  (graph-diff [this other]
    (let [s-po (remove (fn [[s po]] (= po (get (:spo other) s)))
                       spo)]
      (map first s-po)))
  (resolve-triple [this subj pred obj]
    (if-let [[plain-pred trans-tag] (common/check-for-transitive pred)]
      (common/get-transitive-from-index this trans-tag subj plain-pred obj)
      (get-from-multi-index this subj pred obj)))
  (count-triple [this subj pred obj] ;; This intentionally ignores multi-edges, and is used for Naga
    (if-let [[plain-pred trans-tag] (common/check-for-transitive pred)]
      (count-transitive-from-index this trans-tag subj plain-pred obj)
      (common/count-from-index this subj pred obj))))

(defn multi-graph-add
  ([graph subj pred obj n]
   (binding [*insert-op* (partial + n)]
     (graph-add graph subj pred obj)))
  ([graph subj pred obj]
   (binding [*insert-op* inc]
     (graph-add graph subj pred obj))))

(def empty-multi-graph (->MultiGraph {} {} {}))
