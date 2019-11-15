(ns ^{:doc "An in-memory graph implementation with full indexing."
      :author "Paula Gearon"}
    asami.index
  (:require [asami.graph :refer [Graph graph-add graph-delete graph-diff resolve-triple count-triple transitive? plain]]
            [naga.schema.store-structs :as st]
            [clojure.set :as set]
            #?(:clj  [schema.core :as s]
               :cljs [schema.core :as s :include-macros true])))

(def ? :?)

(s/defn index-add :- {s/Any {s/Any #{s/Any}}}
  "Add elements to a 3-level index"
  [idx :- {s/Any {s/Any #{s/Any}}}
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

(defn simplify [g & ks] (map #(if (st/vartest? %) ? :v) ks))

(defmulti get-from-index
  "Lookup an index in the graph for the requested data.
   Returns a sequence of unlabelled bindings. Each binding is a vector of binding values."
  simplify)

;; Extracts the required index (idx), and looks up the requested fields.
;; If an embedded index is pulled out, then this is referred to as edx.
(defmethod get-from-index [:v :v :v] [{idx :spo} s p o] (if (get-in idx [s p o]) [[]] []))
(defmethod get-from-index [:v :v  ?] [{idx :spo} s p o] (map vector (get-in idx [s p])))
(defmethod get-from-index [:v  ? :v] [{idx :osp} s p o] (map vector (get-in idx [o s])))
(defmethod get-from-index [:v  ?  ?] [{idx :spo} s p o] (let [edx (idx s)] (for [p (keys edx) o (edx p)] [p o])))
(defmethod get-from-index [ ? :v :v] [{idx :pos} s p o] (map vector (get-in idx [p o])))
(defmethod get-from-index [ ? :v  ?] [{idx :pos} s p o] (let [edx (idx p)] (for [o (keys edx) s (edx o)] [s o])))
(defmethod get-from-index [ ?  ? :v] [{idx :osp} s p o] (let [edx (idx o)] (for [s (keys edx) p (edx s)] [s p])))
(defmethod get-from-index [ ?  ?  ?] [{idx :spo} s p o] (for [s (keys idx) p (keys (idx s)) o ((idx s) p)] [s p o]))


(defmulti get-transitive-from-index
  "Lookup an index in the graph for the requested data, and returns all data where the required predicate
   is a transitive relationship. Unspecified predicates extend across the graph.
   Returns a sequence of unlabelled bindings. Each binding is a vector of binding values."
  simplify)

;; tests if a transitive path exists between nodes
(defmethod get-transitive-from-index [:v :v :v]
  [{idx :spo} s p o]
  (letfn [(not-solution? [nodes]
            (or (second nodes) ;; more than one result
                (not= o (first nodes)))) ;; single result, but not ending at the terminator
          (edges-from [n] ;; finds all property/value pairs from an entity
            (let [edge-idx (idx s)]
              (for [p (keys edge-idx) o (edge-idx p)] [p o])))
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
          (if (seq next-nodes) [[]] []))))))

;; follows a predicate transitively from a node
(defmethod get-transitive-from-index [:v :v  ?]
  [{idx :spo} s p o]
  (loop [seen? #{} starts [s] result []]
    (let [step (for [s' starts o (get-in idx [s' p]) :when (not (seen? o))] o)]
      (if (empty? step)
        (map vector result)
        (recur (into seen? step) step (concat result step))))))

;; finds all transitive paths that end at a node
(defmethod get-transitive-from-index [ ? :v :v]
  [{idx :pos} s p o]
  (loop [seen? #{} starts [o] result []]
    (let [step (for [o' starts s (get-in idx [p o']) :when (not (seen? s))] s)]
      (if (empty? step)
        (map vector result)
        (recur (into seen? step) step (concat result step))))))

(defn *stream-from
  [selector all-knowns initial-node]
  (letfn [(stream-from [knowns node]
            (let [next-nodes (selector node)
                  next-nodes' (set/difference next-nodes all-knowns)]
              (reduce
               stream-from
               (set/union knowns next-nodes')
               next-nodes')))]
    (stream-from all-knowns initial-node)))

(defn downstream-from
  ([idx node] (downstream-from idx #{} node))
  ([idx all-knowns node]
   (*stream-from #(apply set/union (vals (idx %1))) all-knowns node)))

(defn upstream-from
  ([osp node] (downstream-from osp #{} node))
  ([idx all-knowns node]
   (*stream-from #(keys (idx %1)) all-knowns node)))

;; entire graph from a node
(defmethod get-transitive-from-index [:v  ?  ?]
  [{idx :spo} s p o]
  (let [s-idx (idx s)]
    (for [pred (keys s-idx)
          obj (let [objs (s-idx pred)]
                (concat objs (reduce (partial downstream-from idx) #{} objs)))]
      [pred obj])))

;; entire graph that ends at a node
(defmethod get-transitive-from-index [ ?  ? :v]
  [{idx :osp pos :pos} s p o]
  (for [pred (keys pos)
        subj (let [subjs (get-in pos [pred o])]
               (concat subjs (reduce (partial upstream-from idx) #{} subjs)))]
    [subj pred]))

;; finds a path between 2 nodes
(defmethod get-transitive-from-index [:v  ? :v]
  [{idx :spo} s p o]
  (letfn [(not-solution? [path-nodes]
            (or (second path-nodes) ;; more than one result
                (not= o (second (first path-nodes))))) ;; single result, but not ending at the terminator
          (edges-from [n] ;; finds all property/value pairs from an entity
            (let [edge-idx (idx n)]
              (for [p (keys edge-idx) o (edge-idx p)] [p o])))
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
          (map vector (ffirst next-paths)))))))

;; every node that can reach every node with just a predicate
(defmethod get-transitive-from-index [ ? :v  ?]
  [{idx :pos} s p o]
  ;; function to add an extra step to a current resolution
  (letfn [(step [resolution]
            ;; for each subject node...
            (loop [[s & ss] (keys resolution) result resolution]
              (if s
                ;; for each object associated with the current subject...
                (let [next-result (loop [[o & os] (result s) s-result result]
                                    (if o
                                      ;; find all connections for this object with the current predicate
                                      (let [next-result (if-let [next-os (result o)]
                                                          ;; add all of these to the resolution
                                                          ;; consider only adding if there are things to add
                                                          (update s-result s into next-os)
                                                          s-result)]
                                        (recur os next-result))
                                      s-result))]
                  (recur ss next-result))
                result)))]
    (let [result-index (loop [result (idx p)]
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
  [{idx :spo} s p o]
  (throw (ex-info "Unable to do transitive closure with nothing bound" {:args [s p o]})))


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


(defmulti count-transitive-from-index
  "Lookup an index in the graph for the requested data and count the results based on a transitive index."
  simplify)

(comment "all count-transitive-from-index defmethods")


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
    (if (transitive? pred)
      (get-transitive-from-index this subj (plain pred) obj)
      (get-from-index this subj pred obj)))
  (count-triple [this subj pred obj]
    (if (transitive? pred)
      (count-transitive-from-index this subj (plain pred) obj)
      (count-from-index this subj pred obj))))

(def empty-graph (->GraphIndexed {} {} {}))
