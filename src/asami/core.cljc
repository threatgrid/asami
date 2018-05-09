(ns ^{:doc "A storage implementation over in-memory indexing. Includes full query engine."
      :author "Paula Gearon"}
    asami.core
    #?(:clj (:refer-clojure :exclude [eval]))
    (:require [clojure.set :as set]
              [clojure.string :as str]
              [naga.schema.store-structs :as st
                                         :refer [EPVPattern FilterPattern Pattern
                                                 Results Value Axiom]]
              [naga.util :as u]
              [naga.storage.store-util :as store-util]
              [asami.index :as mem]
              [asami.util :refer [c-eval]]
              [naga.store :as store :refer [Storage StorageType]]
              [naga.store-registry :as registry]
              #?(:clj  [schema.core :as s]
                 :cljs [schema.core :as s :include-macros true])
              #?(:clj [clojure.core.cache :as c])
              #?(:cljs [cljs.core :refer [Symbol PersistentVector List LazySeq]]))
    #?(:clj
        (:import [clojure.lang Symbol IPersistentVector IPersistentList])))

(defprotocol Constraint
  (get-vars [c] "Returns a seq of the vars in a constraint")
  (left-join [c r g] "Left joins a constraint onto a result. Arguments in reverse order to dispatch on constraint type"))


(s/defn without :- [s/Any]
  "Returns a sequence minus a specific element"
  [e :- s/Any
   s :- [s/Any]]
  (remove (partial = e) s))

(s/defn find-start :- EPVPattern
  "Returns the first pattern with the smallest count"
  [pattern-counts :- {EPVPattern s/Num}
   patterns :- [EPVPattern]]
  (let [local-counts (select-keys pattern-counts patterns)
        low-count (reduce min (map second local-counts))
        pattern (ffirst (filter #(= low-count (second %)) local-counts))]
    ;; must use first/filter/= instead of some/#{pattern} because
    ;; patterns contains metadata and pattern does not
    (first (filter (partial = pattern) patterns))))

(s/defn paths :- [[EPVPattern]]
  "Returns a seq of all paths through the constraints. A path is defined
   by new patterns containing at least one variable common to the patterns
   that appeared before it. Patterns must form a group."
  ([patterns :- [EPVPattern]
    pattern-counts :- {EPVPattern s/Num}]
   (s/letfn [(remaining-paths :- [[EPVPattern]]
               [bound :- #{Symbol}
                rpatterns :- [EPVPattern]]
               (if (seq rpatterns)
                 (apply concat
                        (keep ;; discard paths that can't proceed (they return nil)
                         (fn [p]
                           (let [b (get-vars p)]
                             ;; only proceed when the pattern matches what has been bound
                             (if (or (empty? bound) (seq (set/intersection b bound)))
                               ;; pattern can be added to the path, get the other patterns
                               (let [remaining (without p rpatterns)]
                                 ;; if there are more patterns to add to the path, recurse
                                 (if (seq remaining)
                                   (map (partial cons p)
                                        (seq
                                         (remaining-paths (into bound b) remaining)))
                                   [[p]])))))
                         rpatterns))
                 [[]]))]
     (let [start (find-start pattern-counts patterns)
           all-paths (map (partial cons start)
                          (remaining-paths (get-vars start) (without start patterns)))]
       (assert (every? (partial = (count patterns)) (map count all-paths))
               (str "No valid paths through: " (vec patterns)))
       all-paths))))


(def epv-pattern? vector?)
(def filter-pattern? list?)

(s/defn merge-filters
  "Merges filters into the sequence of patterns, so that they appear
   as soon as all their variables are first bound"
  [epv-patterns filter-patterns]
  (let [filter-vars (u/mapmap get-vars filter-patterns)
        all-bound-for? (fn [bound fltr] (every? bound (filter-vars fltr)))]
    (loop [plan [] bound #{} [np & rp :as patterns] epv-patterns filters filter-patterns]
      (if-not (seq patterns)
        ;; no patterns left, so apply remaining filters
        (concat plan filters)

        ;; divide the filters into those which are fully bound, and the rest
        (let [all-bound? (partial all-bound-for? bound)
              nxt-filters (filter all-bound? filters)
              remaining-filters (remove all-bound? filters)]
          ;; if filters were bound, append them, else get the next EPV pattern
          (if (seq nxt-filters)
            (recur (into plan nxt-filters) bound patterns remaining-filters)
            (recur (conj plan np) (into bound (get-vars np)) rp filters)))))))

(s/defn first-group* :- [(s/one [Pattern] "group") (s/one [Pattern] "remainder")]
  "Finds a group from a sequence of patterns. A group is defined by every pattern
   sharing at least one var with at least one other pattern. Returns a pair.
   The first returned element is the Patterns in the group, the second is what was left over."
  [[fp & rp] :- [Pattern]]
  (letfn [;; Define a reduction step.
          ;; Accumulates a triple of: known vars; patterns that are part of the group;
          ;; patterns that are not in the group. Each step looks at a pattern for
          ;; inclusion or exclusion
          (step [[vs included excluded] next-pattern]
            (let [new-vars (get-vars next-pattern)]
              (if (seq (set/intersection vs new-vars))
                [(into vs new-vars) (conj included next-pattern) excluded]
                [vs included (conj excluded next-pattern)])))
          ;; apply the reduction steps, with a given set of known vars, and
          ;; included patterns. Previously excluded patterns are being scanned
          ;; again using the new known vars.
          (groups [[v i e]] (reduce step [v i []] e))]
    ;; scan for everything that matches the first pattern, and then iterate until
    ;; everything that matches the resulting patterns has also been found.
    ;; Drop the set of vars before returning.
    (rest (u/fixpoint groups [(get-vars fp) [fp] rp]))))

(def first-group (memoize first-group*))

(s/defn min-join-path :- [EPVPattern]
  "Calculates a plan based on no outer joins (a cross product), and minimized joins.
   A plan is the order in which to evaluate constraints and join them to the accumulated
   evaluated data. If it is not possible to create a path without a cross product,
   then return a plan of the patterns in the provided order."
  [patterns :- [Pattern]
   count-map :- {EPVPattern s/Num}]
  (loop [[grp rmdr] (first-group patterns) ordered []]
    (let [all-ordered (->> (paths grp count-map)
                           (sort-by (partial mapv count-map))
                           first
                           (concat ordered))] ;; TODO: order groups, rather than concat as found
      (if (empty? rmdr)
        all-ordered
        (recur (first-group rmdr) all-ordered)))))

(s/defn user-plan :- [EPVPattern]
  "Returns the original path specified by the user"
  [patterns :- [EPVPattern]
   _ :- {EPVPattern s/Num}]
  patterns)

(s/defn select-planner
  "Selects a query planner function"
  [options]
  (let [opt (set options)]
    (case (get opt :planner)
      :user user-plan
      :min min-join-path
      min-join-path)))

(s/defn modify-pattern :- [s/Any]
  "Creates a new EPVPattern from an existing one, based on existing bindings.
   Uses the mapping to copy from columns in 'existing' to overwrite variables in 'pattern'.
   The variable locations have already been found and are in the 'mapping' argument"
  [existing :- [Value]
   mapping :- {s/Num s/Num}
   pattern :- EPVPattern]
  ;; TODO: this is in an inner loop. Is it faster to:
  ;;       (reduce (fn [p [f t]] (assoc p f t)) pattern mapping)
  (map-indexed (fn [n v]
                 (if-let [x (mapping n)]
                   (nth existing x)
                   v))
               pattern))

(s/defn pattern-left-join :- Results
  "Takes a partial result, and joins on the resolution of a pattern"
  [graph
   part :- Results
   pattern :- EPVPattern]
  (let [cols (:cols (meta part))
        total-cols (->> (st/vars pattern)
                        (remove (set cols))
                        (concat cols)
                        (into []))
        pattern->left (store-util/matching-vars pattern cols)]
    ;; iterate over part, lookup pattern
    (with-meta
      (for [lrow part
            :let [lookup (modify-pattern lrow pattern->left pattern)]
            rrow (mem/resolve-pattern graph lookup)]
        (concat lrow rrow))
      {:cols total-cols})))

(s/defn filter-join
  "Filters down results."
  [graph
   part :- Results
   fltr :- FilterPattern]
  (let [m (meta part)
        vars (vec (:cols m))
        filter-fn (c-eval (list 'fn [vars] fltr))]
    (with-meta (filter filter-fn part) m)))

(s/defn find-vars [f] (set (filter st/vartest? f)))

;; protocol dispatch for patterns and filters in queries
#?(
:clj
(extend-protocol Constraint
  ;; EPVPatterns are implemented in vectors
  IPersistentVector
  (get-vars [p] (set (st/vars p)))

  (left-join [p results graph] (pattern-left-join graph results p))

  ;; Filters are implemented in lists
  IPersistentList
  (get-vars [f] (or (:vars (meta f)) (find-vars f)))

  (left-join [f results graph] (filter-join graph results f)))

:cljs
(extend-protocol Constraint
  ;; EPVPatterns are implemented in vectors
  PersistentVector
  (get-vars [p] (set (st/vars p)))

  (left-join [p results graph] (pattern-left-join graph results p))

  ;; Filters are implemented in lists
  List
  (get-vars [f] (or (:vars (meta f)) (find-vars f)))
  (left-join [f results graph] (filter-join graph results f))
  
  ;; Clojurescript needs to handle various lists separately
  EmptyList
  (get-vars [f] (or (:vars (meta f)) (find-vars f)))
  (left-join [f results graph] (filter-join graph results f))
  LazySeq
  (get-vars [f] (or (:vars (meta f)) (find-vars f)))
  (left-join [f results graph] (filter-join graph results f))))


(s/defn plan-path :- [(s/one [Pattern] "Patterns in planned order")
                      (s/one {EPVPattern Results} "Single patterns mapped to their resolutions")]
  "Determines the order in which to perform the elements that go into a query.
   Tries to optimize, so it uses the graph to determine some of the
   properties of the query elements. Options can describe which planner to use.
   Planning will determine the resolution map, and this is returned with the plan.
   By default the min-join-path function is used. This can be overriden with options:
     [:planner plan]
   The plan can be one of :user, :min.
   :min is the default. :user means to execute in provided order."
  [graph
   patterns :- [Pattern]
   options]
  (let [epv-patterns (filter epv-pattern? patterns)
        filter-patterns (filter filter-pattern? patterns)

        resolution-map (u/mapmap (partial mem/resolve-pattern graph)
                                 epv-patterns)

        count-map (u/mapmap (comp count resolution-map) epv-patterns)

        query-planner (select-planner options)

        ;; run the query planner
        planned (query-planner epv-patterns count-map)
        plan (merge-filters planned filter-patterns)]

    ;; result
    [plan resolution-map]))


(s/defn join-patterns :- Results
  "Joins the resolutions for a series of patterns into a single result."
  [graph
   patterns :- [Pattern]
   & options]
  (let [[[fpath & rpath] resolution-map] (plan-path graph patterns options)
        ;; execute the plan by joining left-to-right
        ;; left-join has back-to-front params for dispatch reasons
        ljoin #(left-join %2 %1 graph)
        part-result (with-meta
                      (resolution-map fpath)
                      {:cols (st/vars fpath)})]
    (reduce ljoin part-result rpath)))

(s/defn add-to-graph
  [graph
   data :- Results]
  (reduce (fn [acc d] (apply mem/graph-add acc d)) graph data))

#?(:clj
  ;; Using a cache of 1 is currently redundant to an atom
  (let [m (atom (c/lru-cache-factory {} :threshold 1))]
    (defn get-count-fn
      "Returns a memoized counting function for the current graph.
       These functions only last as long as the current graph."
      [graph]
      (if-let [f (c/lookup @m graph)]
        (do
          (swap! m c/hit graph)
          f)
        (let [f (memoize #(count (mem/resolve-pattern graph %)))]
          (swap! m c/miss graph f)
          f))))

  :cljs
  (let [m (atom {})]
    (defn get-count-fn
      "Returns a memoized counting function for the current graph.
       These functions only last as long as the current graph."
      [graph]
      (if-let [f (get @m graph)]
        f
        (let [f (memoize #(count (mem/resolve-pattern graph %)))]
          (reset! m {graph f})
          f)))))

(defrecord MemoryStore [before-graph graph]
  Storage
  (start-tx [this] (->MemoryStore graph graph))

  (commit-tx [this] this)

  (deltas [this]
    ;; sort responses by the number in the node ID, since these are known to be ordered
    (when-let [previous-graph (or (:data (meta this)) before-graph)]
      (->> (mem/graph-diff graph previous-graph)
           (filter (fn [s] (seq (mem/resolve-pattern graph [s :naga/entity '?]))))
           (sort-by #(subs (name %) 5)))))

  (new-node [this]
    (->> "node-"
        gensym
        name
        (keyword "mem")))

  (node-id [this n]
    (subs (name n) 5))

  (node-type? [this prop value]
    (and (keyword? value)
         (= "mem" (namespace value))
         (str/starts-with? (name value) "node-")))

  (data-property [_ data]
    :naga/first)

  (container-property [_ data]
    :naga/contains)

  (resolve-pattern [_ pattern]
    (mem/resolve-pattern graph pattern))

  (count-pattern [_ pattern]
    (if-let [count-fn (get-count-fn graph)]
      (count-fn pattern)
      (mem/count-pattern graph pattern)))

  (query [this output-pattern patterns]
    (store-util/project this output-pattern (join-patterns graph patterns)))

  (assert-data [_ data]
    (->MemoryStore before-graph (add-to-graph graph data)))

  (assert-schema-opts [this _ _] this)

  (query-insert [this assertion-patterns patterns]
    (letfn [(ins-project [data]
              (let [cols (:cols (meta data))]
                (store-util/insert-project this assertion-patterns cols data)))]
      (->> (join-patterns graph patterns)
           ins-project
           (add-to-graph graph)
           (->MemoryStore before-graph)))))

(def empty-store (->MemoryStore nil mem/empty-graph))

(s/defn create-store :- StorageType
  "Factory function to create a store"
  [config]
  empty-store)

(registry/register-storage! :memory create-store)
