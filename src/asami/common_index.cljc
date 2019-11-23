(ns ^{:doc "Common elements for the standard indexed graph,
and multigraph implementations."
      :author "Paula Gearon"}
    asami.common-index
  (:require [asami.graph :refer [Graph graph-add graph-delete graph-diff resolve-triple count-triple]]
            [naga.schema.store-structs :as st]
            [clojure.set :as set]
            #?(:clj  [schema.core :as s]
               :cljs [schema.core :as s :include-macros true])))

(defprotocol NestedIndex
  (lowest-level-fn [this] "Returns a function for handling the lowest index level retrieval")
  (lowest-level-sets-fn [this] "Returns a function retrieving all lowest level values as sets")
  (lowest-level-set-fn [this] "Returns a function retrieving a lowest level value as a set")
  (mid-level-map-fn [this] "Returns a function that converts the mid->lowest in a simple map"))

(def ? :?)

(defn simplify [g & ks] (map #(if (st/vartest? %) ? :v) ks))

(defn trans-simplify [g tag & ks] (map #(if (st/vartest? %) ? :v) ks))


;; These count functions are common, since both indexes use this form of counting for Naga rules
(defn count-embedded-index
  "Adds up the counts of embedded indexes"
  [edx]
  (apply + (map count (vals edx))))

(defmulti count-from-index
  "Lookup an index in the graph for the requested data and count the results."
  simplify)

(defmethod count-from-index [:v :v :v] [{idx :spo} s p o] (if (get-in idx [s p o]) 1 0))
(defmethod count-from-index [:v :v  ?] [{idx :spo} s p o] (count (get-in idx [s p])))
(defmethod count-from-index [:v  ? :v] [{idx :osp} s p o] (count (get-in idx [o s])))
(defmethod count-from-index [:v  ?  ?] [{idx :spo} s p o] (count-embedded-index (idx s)))
(defmethod count-from-index [ ? :v :v] [{idx :pos} s p o] (count (get-in idx [p o])))
(defmethod count-from-index [ ? :v  ?] [{idx :pos} s p o] (count-embedded-index (idx p)))
(defmethod count-from-index [ ?  ? :v] [{idx :osp} s p o] (count-embedded-index (idx o)))
(defmethod count-from-index [ ?  ?  ?] [{idx :spo} s p o] (apply + (map count-embedded-index (vals idx))))

;; transitive predicate management

(def Predicate (s/cond-pre s/Keyword st/Var s/Str))

(s/defn plain :- Predicate
  "Converts a transitive-structured predicate into a plain one"
  [pred :- Predicate]
  (let [nm (name pred)]
    (or
     (and (#{\* \+} (last nm))
          (let [trunc-name (subs nm 0 (dec (count nm)))]
            (cond (keyword? pred) (keyword (namespace pred) trunc-name)
                  (symbol? pred) (symbol (namespace pred) trunc-name)
                  (string? pred) trunc-name)))
     pred)))

(defn second-last
  "Returns the second-last character in a string."
  [s]
  (let [c (count s)]
    (when (< 1 c)
      (nth s (- c 2)))))

(defn check-for-transitive
  "Tests if a predicate is transitive.
  Returns a plain version of the predicate, along with a value to indicate if the predicate is transitive.
  This value is nil for a plan predicate, or else is a keyword to indicate the kind of transitivity."
  [pred]
  (let [{trans? :trans :as meta-pred} (meta pred)
        not-trans? (and (contains? meta-pred :trans) (not trans?))
        pname (name pred)
        tagged (and (not= \' (second-last pname)) ({\* :star \+ :plus} (last pname)))]
    (if not-trans?
      (when tagged [(plain pred) tagged])
      (if tagged
        [(plain pred) tagged]
        (and trans?
             [pred (get #{:star :plus} trans? :star)])))))

;; calculating transitive predicates

(defmulti get-transitive-from-index
  "Lookup an index in the graph for the requested data, and returns all data where the required predicate
   is a transitive relationship. Unspecified predicates extend across the graph.
   Returns a sequence of unlabelled bindings. Each binding is a vector of binding values."
  trans-simplify)

;; tests if a transitive path exists between nodes
(defmethod get-transitive-from-index [:v :v :v]
  [{idx :spo :as graph} tag s p o]
  (let [get-objects (lowest-level-fn graph)]
    (letfn [(not-solution? [nodes]
              (or (second nodes) ;; more than one result
                  (not= o (first nodes)))) ;; single result, but not ending at the terminator
            (edges-from [n] ;; finds all property/value pairs from an entity
              (let [edge-idx (idx s)]
                (for [p (keys edge-idx) o (get-objects (edge-idx p))] [p o])))
            (step [nodes already-seen]
              ;; Steps beyond each node to add all each value for the request properties as the next nodes
              ;; If the node being added is the terminator, then a solution was found and is returned
              (loop [[node & rnodes] nodes result [] seen already-seen]
                (let [[next-result next-seen] (loop [[[p' o' :as edge] & redges] (edges-from node) edge-result result seen? seen]
                                                (if edge
                                                  (if (= o o')
                                                    [[o'] nil] ;; first solution, terminate early
                                                    (if (seen? o')
                                                      (recur redges edge-result seen?)
                                                      (recur redges (conj edge-result o') (conj seen? o'))))
                                                  [edge-result seen?]))]
                  (if (not-solution? next-result)
                    (recur rnodes next-result next-seen)
                    [next-result next-seen]))))] ;; solution found, or else empty result found
      (loop [nodes [s] seen #{}]
        (let [[next-nodes next-seen] (step nodes seen)]
          (if (not-solution? next-nodes)
            (recur next-nodes next-seen)
            (if (seq next-nodes) [[]] [])))))))

;; follows a predicate transitively from a node
(defmethod get-transitive-from-index [:v :v  ?]
  [{idx :spo :as graph} tag s p o]
  (let [get-objects (lowest-level-fn graph)]
    (loop [seen? #{} starts [s] result []]
      (let [step (for [s' starts o (get-objects (get-in idx [s' p])) :when (not (seen? o))] o)]
        (if (empty? step)
          (map vector result)
          (recur (into seen? step) step (concat result step)))))))

;; finds all transitive paths that end at a node
(defmethod get-transitive-from-index [ ? :v :v]
  [{idx :pos :as graph} tag s p o]
  (let [get-subjects (lowest-level-fn graph)]
    (loop [seen? #{} starts [o] result []]
      (let [step (for [o' starts s (get-subjects (get-in idx [p o'])) :when (not (seen? s))] s)]
        (if (empty? step)
          (map vector result)
          (recur (into seen? step) step (concat result step)))))))

(def counter (atom 0))

(defn *stream-from
  [selector all-knowns initial-node]
  (letfn [(stream-from [knowns node]
            (let [next-nodes (selector node)
                  next-nodes' (set/difference next-nodes knowns)
                  knowns' (set/union knowns next-nodes')]
              (reduce
               stream-from
               knowns'
               next-nodes')))]
    (stream-from all-knowns initial-node)))

(defn downstream-from
  ([idx get-object-sets-fn node] (downstream-from idx get-object-sets-fn #{} node))
  ([idx get-object-sets-fn all-knowns node]
   (*stream-from #(apply set/union (get-object-sets-fn (vals (idx %1)))) all-knowns node)))

(defn upstream-from
  ([osp node] (downstream-from osp #{} node))
  ([osp all-knowns node]
   (*stream-from #(set (keys (osp %1))) all-knowns node)))

;; entire graph from a node
(defmethod get-transitive-from-index [:v  ?  ?]
  [{idx :spo :as graph} tag s p o]
  (let [object-sets-fn (lowest-level-sets-fn graph)
        object-set-fn (lowest-level-set-fn graph)
        s-idx (idx s)]
    (for [pred (keys s-idx)
          obj (let [objs (object-set-fn (s-idx pred))]
                (concat objs (reduce (partial downstream-from idx object-sets-fn) #{} objs)))]
      [pred obj])))

;; entire graph that ends at a node
(defmethod get-transitive-from-index [ ?  ? :v]
  [{idx :osp pos :pos :as graph} tag s p o]
  (let [get-subjects (lowest-level-fn graph)]
    (for [pred (keys pos)
          subj (let [subjs (get-subjects (get-in pos [pred o]))]
                 (concat subjs (reduce (partial upstream-from idx) #{} subjs)))]
      [subj pred])))

;; finds a path between 2 nodes
(defmethod get-transitive-from-index [:v  ? :v]
  [{idx :spo :as graph} tag s p o]
  (let [get-objects (lowest-level-fn graph)]
    (letfn [(not-solution? [path-nodes]
              (and (seq path-nodes)
                   (or (second path-nodes) ;; more than one result
                       (not= o (second (first path-nodes)))))) ;; single result, but not ending at the terminator
            (edges-from [n] ;; finds all property/value pairs from an entity
              (let [edge-idx (idx n)]
                (for [p (keys edge-idx) o (get-objects (edge-idx p))] [p o])))
            (step [path-nodes seen]
              ;; Extends path/node pairs to add all each property of the node to the path
              ;; and each associated value as the new node for that path.
              ;; If the node being added is the terminator, then the current path is the solution
              ;; and only that solution is returned, dropping everything else
              (loop [[[path node :as path-node] & rpathnodes] path-nodes result [] seen* seen]
                (if path-node
                  (let [[next-result next-seen] (loop [[[p' o' :as edge] & redges] (edges-from node) edge-result result seen? seen*]
                                                  (if edge
                                                    (if (seen? o')
                                                      (recur redges edge-result seen?)
                                                      (let [new-path-node [(conj path p') o']]
                                                        (if (= o o')
                                                          [[new-path-node] seen?] ;; first solution, terminate early
                                                          (recur redges (conj edge-result new-path-node) (conj seen? o')))))
                                                    [edge-result seen?]))]
                    (if (not-solution? next-result)
                      (recur rpathnodes next-result next-seen)
                      [next-result next-seen]))
                  [result seen*])))] ;; solution found, or else empty result found
      (loop [paths [[[] s]] seen #{}]
        (let [[next-paths next-seen] (step paths seen)]
          (if (not-solution? next-paths)
            (recur next-paths next-seen)
            (map vector (ffirst next-paths))))))))

;; every node that can reach every node with just a predicate
(defmethod get-transitive-from-index [ ? :v  ?]
  [{idx :pos :as graph} tag s p o]
  ;; function to add an extra step to a current resolution
  (letfn [(step [resolution]
            ;; for each object node...
            (loop [[o & os] (keys resolution) result resolution]
              (if o
                ;; for each subject associated with the current object...
                (let [next-result (loop [[s & ss] (result o) o-result result]
                                    (if s
                                      ;; find all connections for this object with the current predicate
                                      (let [next-result (if-let [next-ss (result s)]
                                                          ;; add all of these to the resolution
                                                          ;; consider only adding if there are things to add
                                                          (update o-result o into next-ss)
                                                          o-result)]
                                        (recur ss next-result))
                                      o-result))]
                  (recur os next-result))
                result)))]
    (let [o->s-map (mid-level-map-fn graph)
          result-index (loop [result (o->s-map (idx p))]
                         (let [next-result (step result)]
                           ;; note: consider a faster comparison
                           (if (= next-result result)
                             result
                             (recur next-result))))]
      (for [s' (keys result-index) o' (result-index s')]
        [s' o']))))

;; every node that can reach every node
;; expensive and pointless, so throw exception
(defmethod get-transitive-from-index [ ?  ?  ?]
  [{idx :spo} tag s p o]
  (throw (ex-info "Unable to do transitive closure with nothing bound" {:args [s p o]})))
