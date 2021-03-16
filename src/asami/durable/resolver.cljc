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

(def project-after-first #(subvec % 1 3))

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


(defn transitive-from
  "Steps out from a provided node either forwards or backwards, to the next nodes in the required direction.
  idx: The index to use for lookups in the required direction
  pool: The datapool to turn local nodes (numbers) into global nodes (values)
  tag: Indicates is the transitive operation is * or +
  x: The starting node. A subject when going downstream, or an object when going upstream.
  tproject: A function for projecting tuples of predicates and the next node from x.
  ypos: The position in the index of the next nodes in the required direction.
        subjects when starting at an object, and objects when starting at a subject.
  rproject: A function for projecting the final result as a vector in the expected order for the operation."
  [idx pool tag x tproject ypos rproject]
  (let [f-obj (partial find-object pool)
        x-val (f-obj x)
        starred (= :star tag) ;; was the transitive operation * or +
        tuples (map tproject (find-tuple idx [x]))
        ;; the following includes state, but this is in order to stay relatively lazy
        all-pred (volatile! {})
        knowns (volatile! #{})]
    (for [[pred y] tuples
          y' (do
               (when-not (@all-pred pred) ;; when the predicate changes
                 (vreset! knowns #{y}) ;; start with a fresh set of known nodes
                 (vswap! all-pred assoc pred (f-obj pred))) ;; remember the new predicate & its global form
               ;; accumulate all nodes up/down-stream from the object node
               (*stream-from (fn [x] (into #{} (map #(nth % ypos) (find-tuple idx [x]))))
                             knowns y)
               ;; extract the accumulated nodes. Add the zero-step node if needed
               (conj (if starred (conj @knowns x) @knowns) y))]
      ;; emit the global forms of the predicate and each object
      (rproject (@all-pred pred) (f-obj y')))))

;; entire graph from a node
;; the predicates returned are the first step in the path
;; consider the entire path, as per the [:v ? :v] function
(defmethod get-transitive-from-index [:v  ?  ?]
  [{idx :spot pool :pool :as graph} tag s p o]
  (transitive-from idx pool tag s project-after-first 2 vector))

;; entire graph that ends at a node
(defmethod get-transitive-from-index [ ?  ? :v]
  [{idx :ospt pos :pos pool :pool :as graph} tag s p o]
  (transitive-from idx pool tag o (fn [[_ s p]] [p s]) 1 (fn [pr sb] [sb pr])))

(defn ordered-collect
  "Converts a sequence of key/value pairs that are grouped by key, and returns a map of keys to sets of values.
  The grouping of keys allows the avoidance of map lookups."
  [pairs]
  (loop [[[k v :as p] & rpairs] pairs prev-key nil vls #{} result {}]
    (if-not p
      (if prev-key (assoc result prev-key vls) result)
      (if (= k prev-key)
        (recur rpairs prev-key (conj vls v) result)
        (recur rpairs k (conj #{} v) (if prev-key (assoc result prev-key vls) result))))))

;; every node that can reach every node with a specified predicate
;; This result is in-memory. It can be done with lazy joins, but will be significantly slower
;; Revist this is scalability becomes an issue
(defmethod get-transitive-from-index [ ? :v  ?]
  [{idx :post :as graph} tag s p o]
  (let [os-pairs (map project-after-first (find-tuple idx [p]))
        result-index (loop [result (ordered-collect os-pairs)]
                       (let [next-result (common-index/step-by-predicate result)]
                         ;; note: consider a faster comparison
                         (if (= next-result result)
                           result
                           (recur next-result))))]
    (for [s' (keys result-index) o' (result-index s')]
      [s' o'])))

;; finds a path between 2 nodes
(defmethod get-transitive-from-index [:v  ? :v]
  [{idx :spot :as graph} tag s p o]
  (letfn [(edges-from [n] ;; finds all property/value pairs from an entity
            (map project-after-first (find-tuple idx [n])))]
    (common-index/get-path-between idx edges-from tag s o)))

;; every node that can reach every node
;; expensive and pointless, so throw exception
(defmethod get-transitive-from-index [ ?  ?  ?]
  [graph tag s p o]
  (throw (ex-info "Unable to do transitive closure with nothing bound" {:args [s p o]})))

(defmethod get-transitive-from-index :default
  [graph tag s p o]
  (throw (ex-info (str "Transitive predicates not yet implemented for: " [s p o]) {:s s :p p :o o})))
