(ns ^{:doc "Namespace for functions that plan queries"
      :author "Paula Gearon"}
    asami.planner
    #?(:clj (:refer-clojure :exclude [eval]))
    (:require [clojure.set :as set]
              [naga.schema.store-structs :as st
                                         :refer [EPVPattern Pattern
                                                 epv-pattern? filter-pattern? op-pattern?]]
              [naga.util :as u]
              [asami.graph :as gr]
              #?(:clj  [schema.core :as s]
                 :cljs [schema.core :as s :include-macros true])
              #?(:cljs [cljs.core :refer [Symbol]]))
    #?(:clj
        (:import [clojure.lang Symbol])))


(defprotocol HasVars
  (get-vars [this] "Returns the vars for the object"))

(s/defn without :- [s/Any]
  "Returns a sequence minus a specific element"
  [e :- s/Any
   s :- [s/Any]]
  (remove (partial = e) s))

(s/def Bindings (s/constrained [[s/Any]] #(:cols (meta %))))

(defn bindings?
  [b]
  (and (vector? (:cols (meta b))) (sequential? b)))

(defn nested-seq?
  "Test for Bindings, which can be [] or [[value] [value]...]"
  [s]
  (and (sequential? s) (or (empty? s) (vector? (first s)))))

(def PatternOrBindings (s/conditional nested-seq? Bindings :else Pattern))

(def CountablePattern (s/conditional nested-seq? Bindings :else EPVPattern))

(s/defn find-start :- CountablePattern
  "Returns the first pattern with the smallest count"
  [pattern-counts :- {CountablePattern s/Num}
   patterns :- [CountablePattern]]
  (let [local-counts (select-keys pattern-counts patterns)
        low-count (reduce min (map second local-counts))
        pattern (ffirst (filter #(= low-count (second %)) local-counts))]
    ;; must use first/filter/= instead of some/#{pattern} because
    ;; patterns contains metadata and pattern does not
    (first (filter (partial = pattern) patterns))))

(s/defn paths :- [[CountablePattern]]
  "Returns a seq of all paths through the constraints. A path is defined
   by new patterns containing at least one variable common to the patterns
   that appeared before it. Patterns must form a group.
   This provides all the options for the optimizer to choose from."
  ([patterns :- [CountablePattern]
    pattern-counts :- {CountablePattern s/Num}]
   (s/letfn [(remaining-paths :- [[CountablePattern]]
               [bound :- #{Symbol}
                rpatterns :- [CountablePattern]]
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

(s/defn merge-filters
  "Merges filters into the sequence of patterns, so that they appear
   as soon as all their variables are first bound. By pushing filters as far to the front
   as possible, it minimizes the work of subsequent joins."
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

(s/defn merge-ops
  "Merges operator patterns into the sequence of patterns, so that they appear as soon as all
   their variables are first bound. By pushing operator patterns as close to the front as
   possible, all subsequent uses of the binding have access to them."
  [patterns op-patterns]
  ;; todo: similar to merge-filters above
  ;; for now, operators are going to the back
  (concat patterns op-patterns))

(s/defn first-group* :- [(s/one [CountablePattern] "group") (s/one [CountablePattern] "remainder")]
  "Finds a group from a sequence of patterns. A group is defined by every pattern
   sharing at least one var with at least one other pattern. This is done to group patterns
   by those which can be joined with inner joins. Groups do not share variables, so a join
   from a group to any pattern in a different group will be an outer join. The optimizer
   has to work on one group at a time.
   Returns a pair.
   The first returned element is the Patterns in the group, the second is what was left over.
   This remainder contains all the patterns that appear in other groups. The function can
   be called again on the remainder."
  [[fp & rp :as all] :- [CountablePattern]]
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

(def first-group
  "Queries are often executed multiple times. Memoizing first-group* allows the optimizer
   to avoid redundant work."
  (memoize first-group*))

(s/defn min-join-path :- [CountablePattern]
  "Calculates a plan based on no outer joins (a cross product), and minimized joins.
   A plan is the order in which to evaluate constraints and join them to the accumulated
   evaluated data. If it is not possible to create a path without a cross product,
   then return a plan which is a concatenation of all inner-product groups, where the
   groups are all ordered by minimized joins."
  [patterns :- [CountablePattern]
   count-map :- {CountablePattern s/Num}]
  (if (<= (count patterns) 1)
    patterns
    (loop [[grp rmdr] (first-group patterns) ordered []]
      (let [all-ordered (->> (paths grp count-map)
                             (sort-by (partial mapv count-map))
                             first
                             (concat ordered))]
          ;; TODO: consider ordering groups, rather than concat as found
          ;; since some outer joins may be cheaper than others
        (if (empty? rmdr)
          all-ordered
          (recur (first-group rmdr) all-ordered))))))

(s/defn user-plan :- [CountablePattern]
  "Returns the original order of patterns specified by the user. No optimization is attempted."
  [patterns :- [CountablePattern]
   _ :- {CountablePattern s/Num}]
  patterns)

(s/defn select-planner
  "Selects a query planner function, based on user-selected options"
  [options]
  (let [opt (set options)]
    (case (get opt :planner)
      :user user-plan
      :min min-join-path
      min-join-path)))

(s/defn plan-path :- [PatternOrBindings] ; "Patterns in planned order"
  "Determines the order in which to perform the elements that go into a query.
   Tries to optimize, so it uses the graph to determine some of the
   properties of the query elements. Options can describe which planner to use.
   Planning will determine the resolution map, and this is returned with the plan.
   By default the min-join-path function is used. This can be overriden with options:
     [:planner plan]
   The plan can be one of :user, :min.
   :min is the default. :user means to execute in provided order."
  [graph
   patterns :- [PatternOrBindings]
   options]
  (let [epv-patterns (filter #(and (epv-pattern? %) (not (bindings? %))) patterns)
        prebounds (filter bindings? patterns)
        filter-patterns (filter filter-pattern? patterns)
        op-patterns (filter op-pattern? patterns)

        count-map (merge
                   (u/mapmap (partial gr/count-pattern graph) epv-patterns)
                   (u/mapmap count prebounds))

        query-planner (select-planner options)

        ;; run the query planner
        planned (query-planner (concat prebounds epv-patterns) count-map)
        filtered-plan (merge-filters planned filter-patterns)]

    ;; result
    (merge-ops filtered-plan op-patterns)))
