(ns ^{:doc "Namespace for functions that plan queries"
      :author "Paula Gearon"}
    asami.planner
    #?(:clj (:refer-clojure :exclude [eval]))
    (:require [clojure.set :as set]
              [clojure.string :as str]
              [naga.schema.store-structs :as st
                                         :refer [EPVPattern Pattern EvalPattern Var vartest?
                                                 epv-pattern? filter-pattern? eval-pattern? op-pattern?
                                                 list-like?]]
              [naga.util :as u]
              [asami.graph :as gr]
              #?(:clj  [schema.core :as s]
                 :cljs [schema.core :as s :include-macros true])
              #?(:cljs [cljs.core :refer [Symbol]]))
    #?(:clj
        (:import [clojure.lang Symbol])))

(def latest-time (atom (. System (nanoTime))))

(defn start-time [label]
  (println label)
  (reset! latest-time (. System (nanoTime))))

(defn print-time
  ([label] (print-time label nil))
  ([label x]
   (let [next-time (. System (nanoTime))]
     (println label ": " (/ (double (- next-time @latest-time)) 1000000.0) "ms")
     (reset! latest-time next-time))
   x))


(defprotocol HasVars
  (get-vars [this] "Returns the vars for the object"))

(s/defn without :- [s/Any]
  "Returns a sequence minus a specific element"
  [e :- s/Any
   s :- [s/Any]]
  (remove (partial = e) s))

;; Bindings are when predefined data is given to the query
;; these take the form of a sequence of binding values
;; and the column names in metadata
(s/def Bindings (s/constrained [[s/Any]] #(:cols (meta %))))

(defn bindings?
  [b]
  (and (vector? (:cols (meta b))) (sequential? b)))

(defn nested-seq?
  "Test for Bindings, which can be [] or [[value] [value]...]"
  [s]
  (and (sequential? s) (or (empty? s) (vector? (first s)))))

(def PatternOrBindings (s/conditional nested-seq? Bindings :else Pattern))

(def CountablePattern  ;; fixed bindings, or a mathing pattern for an index, or a binding expression
  (s/if nested-seq? Bindings (s/if (comp list-like? first) EvalPattern EPVPattern)))

(def addset (fnil conj #{}))

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


(s/defn order :- [EvalPattern]
  "Takes a sequence of Evaluation binding patterns and returns them in an internally consistent order"
  [patterns :- [EvalPattern]]
  (loop [chain [] to-bind (set (map second patterns)) remaining patterns]
    (if-not (seq remaining)
      chain
      (let [[[_ b :as p]] (filter #(not-any? to-bind (get-vars %)) remaining)]
        (if (nil? p)
          (throw (ex-info (str "Unable to find valid ordering for bindings: " (into [] remaining))
                          {:bindings patterns :to-bind to-bind}))
          (recur (conj chain p) (disj to-bind b) (without p remaining)))))))


(s/defn paths :- [[CountablePattern]]
  "Returns a seq of all paths through the constraints. A path is defined
   by new patterns containing at least one variable common to the patterns
   that appeared before it. Patterns must form a group.
   Paths can be envisioned as a tree, like:
   A-+--B1-+--C1
     |     |
     |     +--C2
     |
     +--B2-+--C3
           |
           +--C4
   The result of this expands the tree into a flattened form:
   [[A,B1,C1] [A,B1,C2] [A,B2,C3] [A,B2,C4]]
   These paths provide all the options for the optimizer to choose from."
  [prebound :- (s/maybe #{Var})
   patterns :- [CountablePattern]
   pattern-counts :- {CountablePattern s/Num}
   eval-patterns :- [EvalPattern]]
  (let [smallest-count (apply min (vals pattern-counts))]
    (s/letfn [(remaining-paths :- [[CountablePattern]]
                [bound :- #{Symbol}
                 rpatterns :- [CountablePattern]
                 binding-outs :- {Var CountablePattern}]
                (if (seq rpatterns)
                  ;; concatting on all possible path extensions.
                  ;; Instead, only concat the extensions that add no new vars, unless that is empty
                  (apply concat
                         (keep ;; discard paths that can't proceed (they return nil)
                          (fn [p]
                            (let [b (get-vars p)]
                              ;; only proceed when the pattern matches what has been bound,
                              ;; and does not rely on an expression binding
                              (if (and (or (empty? bound) (seq (set/intersection b bound)))
                                       (not (some binding-outs b)))
                                ;; pattern can be added to the path, get the other patterns
                                (let [remaining (without p rpatterns)]
                                  ;; if there are more patterns to add to the path, recurse
                                  (if (seq remaining)
                                    (map (partial cons p)
                                         (seq
                                          (remaining-paths (into bound b) remaining binding-outs)))
                                    [[p]]))
                                ;; can this pattern be added if bindings are brought in?
                                (let [pre-reqs (keep binding-outs b)]
                                  (if (seq pre-reqs)
                                    ;; are all variables for these bindings already available?
                                    (if (every? bound (mapcat get-vars pre-reqs))
                                      ;; sort the bindings that are about to be used
                                      (let [ordered-pre-reqs (order pre-reqs)
                                            ;; all the variables that became bound no longer need to be looked for
                                            remaining-binding-outs (apply dissoc binding-outs b)]
                                        ;; if there are more patterns to add, then recurse
                                        (if-let [remaining (seq (without p rpatterns))]
                                          (map (partial concat ordered-pre-reqs [p])
                                               (seq (remaining-paths (-> bound (into b) (into (map second pre-reqs)))
                                                                     remaining
                                                                     remaining-binding-outs)))
                                          ;; otherwise, return everything
                                          [(concat ordered-pre-reqs [p] (order (vals remaining-binding-outs)))]))))))))
                          rpatterns))
                  [[]]))
              (path-through :- [CountablePattern]
                [bound :- #{Symbol}
                 rpatterns :- [CountablePattern]
                 binding-outs :- {Var CountablePattern}]
                (if (seq rpatterns)
                  (letfn [(possible-next-pattern? [p]
                            (let [b (get-vars p)]
                              ;; only use this pattern when it matches what has been bound,
                              ;; and does not rely on an expression binding
                              (and (or (empty? bound) (seq (set/intersection b bound)))
                                   (not (some binding-outs b)))))
                          ;; test if every var in a pattern has already been bound
                          (all-bound? [p] (every? bound (get-vars p)))
                          ;; return the pattern with the smallest resolution count
                          (min-pattern [ps]
                            (loop [[p & rp] ps m p mcount 0]
                              (if (nil? p)
                                m
                                (let [pcount (pattern-counts p)]
                                  ;; optimization: short circuit when this is known to be the smallest possible value
                                  (if (= smallest-count pcount)
                                    p
                                    (if (or (< pcount mcount) (zero? mcount))
                                      (recur rp p pcount)
                                      (recur rp m mcount)))))))]
                    (let [nexts (filter possible-next-pattern? rpatterns)]
                      (if (seq nexts)
                        (let [fully-bound-nexts (filter all-bound? nexts)
                              next-pattern (if (seq fully-bound-nexts)
                                             (min-pattern fully-bound-nexts)
                                             (min-pattern nexts))]
                          (cons next-pattern
                                (path-through (into bound (get-vars next-pattern))
                                              (without next-pattern rpatterns)
                                              binding-outs)))
                        ;; TODO: bring in bindings
                        (let [pattern->bindings (into {} (keep pattern-bindings-pair rpatterns))]  ;; TODO: pattern-bindings-pair
                          (if (seq pattern->bindings)
                            (let [p (min-pattern (keys pattern->bindings))
                                  bindings (pattern->bindings p)
                                  b (get-vars p)]
                              )
                            (throw (ex-info (str "Unable to find path through: " rpatterns)
                                            {:patterns rpatterns :bound bound :binding-outs binding-outs})))))))))]
      (let [binding-outs (u/mapmap second identity eval-patterns)
            _ (start-time "finding start")
            start (->> (if (seq prebound)
                         (filter (comp (partial some prebound) get-vars) patterns)
                         patterns)
                       (remove (comp (partial some binding-outs) get-vars))
                       (find-start pattern-counts))
                                        ; _ (print-time (str "Got start: " start))
            all-paths (map (partial cons start)
                           (remaining-paths (get-vars start) (without start patterns) binding-outs))]
                                        ; (print-time (str "path count: " (count all-paths)))
        (assert (every? (partial = (count patterns)) (map #(->> % (remove eval-pattern?) count) all-paths))
                (str "No valid paths through: " (vec patterns)))
        all-paths))))

(declare plan-path)

(s/defn merge-operations
  "Merges filters and NOT operations into the sequence of patterns, so that they appear
   as soon as all their variables are first bound. By pushing filters as far to the front
   as possible, it minimizes the work of subsequent joins.
   TODO: if not-patterns relies on the output of an eval-pattern, then the eval can be pushed
   further ahead in the path. This should happen before this merge is called."
  [graph
   options
   planned-patterns
   general-patterns
   filter-patterns
   not-patterns]
  (let [out-vars (fn [p] (if (eval-pattern? p) [(second p)] (get-vars p)))
        all-non-negation-vars (set (mapcat out-vars (concat planned-patterns general-patterns)))
        filter-vars (u/mapmap get-vars (concat filter-patterns not-patterns))
        all-bound-for? (fn [bound fltr]
                         (every? #(or (bound %) (not (all-non-negation-vars %)))
                                 (filter-vars fltr)))
        plan-negation-path (fn [bound [op & patterns]]
                             (apply list op (plan-path graph patterns (assoc options :bound bound))))]
    (loop [plan []
           bound #{}
           [np & rp :as patterns] planned-patterns
           filters filter-patterns
           negations not-patterns]
      (if-not (seq patterns)
        ;; no patterns left, so add remaining general patterns, negations, then filters
        (let [planned-negations (map (partial plan-negation-path bound) negations)]
          (concat plan general-patterns planned-negations filters))

        ;; divide the filters into those which are fully bound, and the rest
        (let [all-bound? (partial all-bound-for? bound)
              nxt-filters (filter all-bound? filters)
              nxt-negations (->> negations
                                 (filter all-bound?)
                                 (map (partial plan-negation-path bound)))
              all-nexts (concat nxt-negations nxt-filters)
              remaining-filters (remove all-bound? filters)
              remaining-negations (remove all-bound? negations)]
          ;; if filters were bound, append them, else get the next EPV pattern
          (if (seq all-nexts)
            (recur (into plan all-nexts) bound patterns remaining-filters remaining-negations)
            (recur (conj plan np) (into bound (out-vars np)) rp filters negations)))))))

(s/defn bindings-chain :- (s/maybe
                           [(s/one [EvalPattern] "eval-patterns that can be used to bind a pattern")
                            (s/one [EvalPattern] "eval-patterns that cannot be used to bind a pattern")])
  "This is a helper function for first-group.
   When first-group has found a set of patterns that share vars with each other, this will look for
   any eval-patterns (binding through evaluation) which, if added, would allow even more patterns
   to be added to the group. So if we had:
   [?a :prop ?b] [?a :attr ?c] [(inc ?c) ?d] [(str ?b \"-\" ?d) ?e] [(dec ?c) ?f] [?x :label ?e] [?y :included true]
   Then first-group would find the first 2 patterns, which would bind: #{?a ?b ?c}
   This function is then called, with:
     evs = [[(inc ?c) ?d] [(str ?b \"-\" ?d) ?e] [(dec ?c) ?f]]
     bound-vars = #{?a ?b ?c}
     patterns = [[?x :label ?e] [?y :included true]]
   It will identify that [?x :label ?e] can be included if the first 2 evaluations are used,
   since they can be used to bind ?e.
   The result will be a split of the bindings that can be used to match a pattern, and the bindings which cannot:
     [ [[(inc ?c) ?d] [(str ?b \"-\" ?d) ?e]] , [[(dec ?c) ?f]] ]
   The loop in first-group will then use this result to pull in all possible patterns.

   Finds all bindings will can be used to connect unused patterns to a group.
   Returns a pair: Bindings that can be used to connect patterns to the current group / Remaining bindings
   evs: eval patterns that are available to use
   bound-vars: the vars that have been bound for the current group
   patterns: the patterns that aren't used in any groups yet"
  [evs :- [EvalPattern]
   bound-vars :- #{Var}
   patterns :- [EPVPattern]]
  (let [out->evals (u/mapmap second identity evs)
        find-incoming (fn [evals acc]
                        (let [incoming (remove bound-vars (mapcat get-vars evals))]
                          (if-not (seq incoming) ;; check if all incoming vars are bound
                            (concat evals acc) ;; all bound, so can use all the eval-patterns that have been found
                            ;; Get all eval-patterns that bind what we need
                            (let [next-evals (map out->evals incoming)]
                              ;; if any eval-patterns need vars that can't be bound then return nil
                              (if (every? identity next-evals)
                                ;; check the next eval-patterns, and add them to our collection
                                (recur next-evals (concat evals acc)))))))]
    (loop [[p & rp] patterns]
      (if p
        ;; for each pattern, find incoming eval-bindings
        (let [evals (keep out->evals (get-vars p))]
          (if-not (seq evals)
            ;; nothing incoming, so go to the next pattern
            (recur rp)
            ;; check if the incoming eval-bindings can be satisfied
            (let [chain-data (find-incoming evals [])]
              (if (seq chain-data)
                (do
                  [chain-data (remove (set chain-data) evs)])
                (recur rp)))))
        [nil evs]))))

(s/defn first-group* :- [(s/one [CountablePattern] "group")
                         (s/one [CountablePattern] "remainder")
                         (s/one [EvalPattern] "unused eval bindings")]
  "Finds a group from a sequence of patterns. A group is defined by every pattern
   sharing at least one var with at least one other pattern. This is done to group patterns
   by those which can be joined with inner joins. Groups do not share variables, so a join
   from a group to any pattern in a different group will be an outer join. The optimizer
   has to work on one group at a time.
   For the following query:
   [?a :prop ?b] [?a :attr ?c] [(inc ?c) ?d] [(str ?b \"-\" ?d) ?e] [(dec ?c) ?f] [?x :label ?e] [?y :included true]
   All patterns except the last one are in the same group.
   Returns a pair.
   The first returned element is the Patterns in the group, the second is what was left over.
   This remainder contains all the patterns that appear in other groups. The function can
   be called again on the remainder."
  [bound :- (s/maybe #{Var})
   patterns :- [CountablePattern]
   eval-patterns :- [EvalPattern]]
  (letfn [ ;; Define a reduction step.
          ;; Accumulates a triple of: known vars; patterns that are part of the group;
          ;; patterns that are not in the group. Each step looks at a pattern for
          ;; inclusion or exclusion
          (step [[vs included excluded] next-pattern]
            (let [new-vars (set (get-vars next-pattern))]
              (if (seq (set/intersection vs new-vars))
                [(into vs new-vars) (conj included next-pattern) excluded]
                [vs included (conj excluded next-pattern)])))
          ;; apply the reduction steps, with a given set of known vars, and
          ;; included patterns. Previously excluded patterns are being scanned
          ;; again using the new known vars.
          (groups [[v i e]] (reduce step [v i []] e))]
    (let [eval-outs (set (map second eval-patterns))
          independents (remove #(some eval-outs (get-vars %)) patterns)
          [first-pattern] (if (seq bound)
                            (filter #(some bound (get-vars %)) independents)
                            independents)]
      (loop [included-vars (set (get-vars first-pattern))
             included [first-pattern]
             excluded (without first-pattern patterns)
             excl-evals eval-patterns]
        ;; scan for everything that matches the first pattern, and then iterate until
        ;; everything that matches the resulting patterns has also been found.
        (let [[in-vars in-group ex-group] (u/fixpoint groups [included-vars included excluded])
              ;; check if the bindings could add more patterns
              [in-evals ex-evals] (bindings-chain excl-evals in-vars ex-group)]
          (if-not (seq in-evals)
            ;; extra eval bindings won't help, so return
            [in-group ex-group excl-evals]
            ;; add the eval bindings that will help, and run again
            (recur (into in-vars (map second in-evals))
                   (vec (concat in-group in-evals))
                   ex-group
                   ex-evals)))))))

(def first-group
  "Queries are often executed multiple times. Memoizing first-group* allows the optimizer
   to avoid redundant work."
  (memoize first-group*))

(s/defn estimated-counts :- [s/Num]
  "Return list of ordered counts for the patterns. This skips the eval-patterns.
  It also attaches meta-data to indicate if a path can short circuit comparisons."
  [count-map :- {CountablePattern s/Num}
   path :- [CountablePattern]]
  (let [counts (into [] (keep count-map path))]
    (with-meta counts {:zero (some zero? counts) :single (every? (partial = 1) counts)})))

(s/defn find-first :- [CountablePattern]
  "Finds a min (or approximate minimum) path"
  [count-map :- {CountablePattern s/Num}
   [first-path & all-paths] :- [[CountablePattern]]]
  (let [count-fn (partial estimated-counts count-map)]
    (loop [min-path first-path min-counts (count-fn first-path) [fpath & rpaths] all-paths]
      (if (or (nil? fpath)
              (let [{:keys [zero single]} (meta min-path)] (or zero single)))
        min-path
        (let [f-min-counts (count-fn fpath)]
          (if (= 1 (compare min-counts f-min-counts))
            (recur fpath f-min-counts rpaths)
            (recur min-path min-counts rpaths)))))))

(s/defn min-join-path :- [CountablePattern]
  "Calculates a plan based on no outer joins (a cross product), and minimized joins.
   A plan is the order in which to evaluate constraints and join them to the accumulated
   evaluated data. If it is not possible to create a path without a cross product,
   then return a plan which is a concatenation of all inner-product groups, where the
   groups are all ordered by minimized joins."
  [bound :- (s/maybe #{Var})
   count-map :- {CountablePattern s/Num}
   patterns :- [CountablePattern]
   eval-patterns :- [EvalPattern]]
  (if (<= (count patterns) 1)
    (concat patterns (order eval-patterns))
    (loop [[grp rmdr evalps] (first-group bound patterns eval-patterns) ordered []]
      (let [group-evals (filter eval-pattern? grp)
            group-countables (remove eval-pattern? grp)
            all-ordered (->> (paths bound group-countables count-map group-evals)
                             (find-first count-map)
                             (concat ordered))]
        (if (empty? rmdr)
          (concat all-ordered (order evalps))
          (recur (first-group bound rmdr evalps) all-ordered))))))

(s/defn not-operation? :- s/Bool
  "Returns true if a pattern is a NOT operation"
  [pattern :- PatternOrBindings]
  (and (list-like? pattern)
       (contains? #{'not 'NOT} (first pattern))))

(s/defn extract-patterns-by-type :- {s/Keyword [PatternOrBindings]}
  "Categorizes elements of a WHERE clause, returning a keyword map"
  [patterns :- [PatternOrBindings]]
  (reduce (fn [acc p]
            (update acc
                    (cond
                      (bindings? p) :prebounds    ;; [[data] [data] [data]...]
                      (epv-pattern? p) :epv-patterns  ;; [?entity ?attribute ?value]
                      (filter-pattern? p) :filter-patterns  ;; [(test ?x)]
                      (eval-pattern? p) :eval-patterns  ;; [(operation ?x) ?y]
                      (not-operation? p) :not-patterns  ;; (not [?entity :attr "value"])
                      (op-pattern? p) :op-patterns  ;; (or [?e :a "v"] [?e :a "w"])
                      :default :unknown)  ;; error
                    (fnil conj []) p))
          {}
          patterns))

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
  (let [{:keys [prebounds epv-patterns filter-patterns
                eval-patterns not-patterns op-patterns unknown] :as p} (extract-patterns-by-type patterns)
        _ (when (seq unknown) (throw (ex-info (str "Unknown form in query: " (str/join "," unknown))
                                              {:unknown unknown :query patterns})))

        count-map (merge
                   (u/mapmap (partial gr/count-pattern graph) epv-patterns)
                   (u/mapmap count prebounds))

        ;; run the query planner
        bound (:bound options)
        ;; TODO: sub patterns use bindings and leave behind bindings
        plan-operation (fn [[op & args :as sub]] (apply list op (plan-path graph args options)))
        planned-sub-patterns (map plan-operation op-patterns)
        planned (min-join-path bound count-map (concat prebounds epv-patterns) eval-patterns)]

    ;; result
    (merge-operations graph options planned planned-sub-patterns filter-patterns not-patterns)))

(s/defn simplify-algebra :- [PatternOrBindings]
  "This operation simplifies the algebra into a sum-of-products form.
   Currently a placeholder that does nothing."
  [patterns :- [PatternOrBindings]
   options]
  patterns)

(s/defn minimal-first-planner :- [PatternOrBindings]
  "Attempts to optimize a query, based on the principle that if smaller resolutions appear
   first in a product term, then lazy evaluation will lead to less iteration on the later terms.
   This is not always true, but is in the general case."
  [graph
   patterns :- [PatternOrBindings]
   options]
  (let [simplified-patterns (simplify-algebra patterns options)]
    (plan-path graph patterns options)))

(s/defn user-plan :- [CountablePattern]
  "Returns the original order of patterns specified by the user. No optimization is attempted."
  [graph
   patterns :- [CountablePattern]
   {:keys [simplify] :as options}]
  (if simplify
    (simplify-algebra patterns options)
    patterns))
