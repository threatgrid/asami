(ns ^{:doc "Handles resolving patterns on a graph"
      :author "Paula Gearon"}
    asami.durable.resolver
  (:require [asami.common-index :as common-index :refer [?]]
            [asami.durable.common :as common :refer [find-tuple find-object]]))

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

(defmethod get-transitive-from-index :default
  [graph tag s p o]
  (throw (ex-info (str "Transitive predicates not yet implemented for: " [s p o]) {:s s :p p :o o})))
