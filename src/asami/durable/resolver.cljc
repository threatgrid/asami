(ns ^{:doc "Handles resolving patterns on a graph"
      :author "Paula Gearon"}
    asami.durable.resolver
  (:require [asami.common-index :as common-index :refer [?]]
            [asami.durable.common :as common :refer [find-tuple find-object]]
            [clojure.set :as set]))

(defmulti get-from-index
  "Lookup an index in the graph for the requested data.
   Returns a sequence of unlabelled bindings. Each binding is a vector of binding values."
  common-index/simplify)

(def v2 (fn [dp t] (vector (find-object dp (nth t 2)))))
(def v12 (fn [dp t] (vector
                     (find-object dp (nth t 1))
                     (find-object dp (nth t 2)))))
(def v21 (fn [dp t] (vector
                     (find-object dp (nth t 2))
                     (find-object dp (nth t 1)))))

;; Extracts the required index (idx), and looks up the requested fields.
;; If an embedded index is pulled out, then this is referred to as edx.
(defmethod get-from-index [:v :v :v]
  [{idx :spot dp :pool} s p o]
  (if (seq (find-tuple idx [s p o])) [[]] []))

(defmethod get-from-index [:v :v  ?]
  [{idx :spot dp :pool} s p o]
  (map (partial v2 dp) (find-tuple idx [s p])))

(defmethod get-from-index [:v  ? :v]
  [{idx :ospt dp :pool} s p o]
  (map (partial v2 dp) (find-tuple idx [o s])))

(defmethod get-from-index [:v  ?  ?]
  [{idx :spot dp :pool} s p o]
  (map (partial v12 dp) (find-tuple idx [s])))

(defmethod get-from-index [ ? :v :v]
  [{idx :post dp :pool} s p o]
  (map (partial v2 dp) (find-tuple idx [p o])))

(defmethod get-from-index [ ? :v  ?]
  [{idx :post dp :pool} s p o]
  (map (partial v21 dp) (find-tuple idx [p])))

(defmethod get-from-index [ ?  ? :v]
  [{idx :ospt dp :pool} s p o]
  (map (partial v12 dp) (find-tuple idx [o])))

(defmethod get-from-index [ ?  ?  ?]
  [{idx :spot dp :pool} s p o]
  (map #(mapv (partial find-object dp) (take 3 %))
       (find-tuple idx [])))

(defn zero-step
  "Prepend a zero step value if the tag requests it"
  [tag pool zero result]
  (if (= :star tag)
    (let [z [(find-object pool zero)]]
      (cons z result))
    result))

(defmulti get-transitive-from-index
  "Lookup an index in the graph for the requested data, and returns all data where the required predicate
   is a transitive relationship. Unspecified predicates extend across the graph.
   Returns a sequence of unlabelled bindings. Each binding is a vector of binding values."
  common-index/trans-simplify)

(defn get-single-from-index
  [idx data-pool tag st p srch]
  (loop [seen? #{} starts [st] result []]
    (let [step (for [st' starts n (map #(nth % 2) (find-tuple idx (srch st'))) :when (not (seen? n))] n)]
      (if (empty? step)
        (->> result
             (map (fn [x] [(find-object data-pool x)]))
             (zero-step tag data-pool st))
        (recur (into seen? step) step (concat result step))))))

;; follows a predicate transitively from a node
(defmethod get-transitive-from-index [:v :v  ?]
  [{idx :spot pool :pool :as graph} tag s p o]
  (get-single-from-index idx pool tag s p (fn [s'] [s' p])))

;; finds all transitive paths that end at a node
(defmethod get-transitive-from-index [ ? :v :v]
  [{idx :post pool :pool :as graph} tag s p o]
  (get-single-from-index idx pool tag o p (fn [o'] [p o'])))


(defn *stream-from
  [selector knowns initial-node]
  (letfn [(stream-from [node]
            (let [next-nodes (selector node)   ;; node is a subject/object, get all objects/subjects
                  next-nodes' (remove @knowns next-nodes)] ;; remove the knowns from the next nodes
              (vswap! knowns into next-nodes') ;; add all these new nodes to the known set
              (doseq [n next-nodes']
                (stream-from n))))]  ;; go to the next step for each node
    (stream-from initial-node)))

;; entire graph from a node
;; the predicates returned are the first step in the path
;; consider the entire path, as per the [:v ? :v] function
(defmethod get-transitive-from-index [:v  ?  ?]
  [{idx :spot pool :pool :as graph} tag s p o]
  (let [f-obj (partial find-object pool)
        s-val (f-obj s)
        starred (= :star tag)  ;; was the transitive operation * or +
        s-tuples (map #(subvec % 1 3) (find-tuple idx [s]))
        ;; the following includes state, but this is in order to stay relatively lazy
        all-pred (volatile! {})
        knowns (volatile! #{})]
    (for [[pred obj] s-tuples
          obj' (do
                 (when-not (@all-pred pred)  ;; when the predicate changes
                   (vreset! knowns #{obj})   ;; start with a fresh set of known nodes
                   (vswap! all-pred assoc pred (f-obj pred)))  ;; remember the new predicate & its global form
                 ;; accumulate all nodes downstream from the object node
                 (*stream-from (fn [x] (into #{} (map #(nth % 2) (find-tuple idx [x]))))
                               knowns obj)
                 ;; extract the accumulated nodes. Add the zero-step node if needed
                 (conj (if starred (conj @knowns s) @knowns) obj))]
      ;; emit the global forms of the predicate and each object
      [(@all-pred pred) (f-obj obj')])))

;; entire graph that ends at a node
(defmethod get-transitive-from-index [ ?  ? :v]
  [{idx :ospt pos :pos pool :pool :as graph} tag s p o]
  (let [f-obj (partial find-object pool)
        o-val (f-obj o)
        starred (= :star tag)
        o-tuples (map #(subvec % 1 3) (find-tuple idx [o]))
        ;; the following includes state, but this is in order to stay relatively lazy
        all-pred (volatile! {})
        knowns (volatile! #{})]
    (for [[subj pred] o-tuples
          subj' (do
                  (when-not (@all-pred pred)
                    (vreset! knowns #{subj})
                    (vswap! all-pred assoc pred (f-obj pred)))
                  (*stream-from (fn [x] (into #{} (map #(nth % 1) (find-tuple idx [x]))))
                                knowns subj)
                  (conj (if starred (conj @knowns o) @knowns) subj))]
      [(f-obj subj') (@all-pred pred)])))


(defmethod get-transitive-from-index :default
  [graph tag s p o]
  (throw (ex-info (str "Transitive predicates not yet implemented for: " [s p o]) {:s s :p p :o o})))
