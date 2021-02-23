(ns ^{:doc "Namespace for functions that plan queries"
      :author "Paula Gearon"}
    asami.planner
    #?(:clj (:refer-clojure :exclude [eval]))
    (:require [clojure.set :as set]
              [clojure.string :as str]
              [zuko.schema :refer [EPVPattern Pattern EvalPattern Var vartest?
                                   epv-pattern? filter-pattern? eval-pattern? op-pattern?]]
              [zuko.util :as u]
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
  (s/if nested-seq? Bindings (s/if (comp seq? first) EvalPattern EPVPattern)))

(def addset (fnil conj #{}))

(s/defn find-start :- (s/maybe CountablePattern)
  "Returns the first pattern with the smallest count"
  [pattern-counts :- {CountablePattern s/Num}
   patterns :- [CountablePattern]]
  (if (seq patterns)
    (let [local-counts (select-keys pattern-counts patterns)
          low-count (reduce min (map second local-counts))
          pattern (ffirst (filter #(= low-count (second %)) local-counts))]
      ;; must use first/filter/= instead of some/#{pattern} because
      ;; patterns contains metadata and pattern does not
      (first (filter (partial = pattern) patterns)))))


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


(s/defn path :- [CountablePattern]
  "Returns an efficient path through the constraints. A path is defined
   by new patterns containing at least one variable common to the patterns
   that appeared before it. Patterns must form a group."
  [prebound :- (s/maybe #{Var})
   patterns :- [CountablePattern]
   pattern-counts :- {CountablePattern s/Num}
   eval-patterns :- [EvalPattern]]
  (let [smallest-count (apply min (vals pattern-counts))]
    (s/letfn [(path-through :- [CountablePattern]
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
                          (all-bound?
                            ([p] (all-bound? nil p))
                            ([extra-bindings p] (every? (into bound extra-bindings) (get-vars p))))
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
                        (let [fully-bound-patterns (filter all-bound? nexts)
                              next-pattern (if (seq fully-bound-patterns)
                                             (min-pattern fully-bound-patterns)
                                             (min-pattern nexts))]
                          (cons next-pattern
                                (path-through (into bound (get-vars next-pattern))
                                              (without next-pattern rpatterns)
                                              binding-outs)))
                        ;; no pattern can be linked, so bring in bindings
                        (let [pattern-prereq-pair (fn [p]
                                                    ;; find all bindings that bind vars in this pattern
                                                    (let [pre-reqs (keep binding-outs (get-vars p))]
                                                      ;; if bindings are found,
                                                      ;; and the vars that the binding depends on all bound?
                                                      (if (and (seq pre-reqs) (every? bound (mapcat get-vars pre-reqs)))
                                                        [p pre-reqs])))
                              pattern->pre-reqs (into {} (keep pattern-prereq-pair rpatterns))]
                          (if (seq pattern->pre-reqs)
                            (let [fully-bound-patterns (keep (fn [[ptn pre-reqs]]
                                                               (if (all-bound? (map second pre-reqs) ptn) ptn))
                                                             pattern->pre-reqs)
                                  p (if (seq fully-bound-patterns)
                                      (min-pattern fully-bound-patterns)
                                      (min-pattern (keys pattern->pre-reqs)))
                                  ordered-pre-reqs (order (pattern->pre-reqs p))
                                  b (get-vars p)
                                  ;; all the variables that became bound no longer need to be looked for
                                  remaining-binding-outs (apply dissoc binding-outs b)]
                              ;; if there are more patterns to add, then recurse
                              (if-let [remaining (seq (without p rpatterns))]
                                (concat ordered-pre-reqs
                                        [p]
                                        (path-through (-> bound (into b) (into (map second ordered-pre-reqs)))
                                                      remaining
                                                      remaining-binding-outs))
                                ;; otherwise, return everything that's left
                                (concat ordered-pre-reqs [p] (order (vals remaining-binding-outs)))))
                            (throw (ex-info (str "Unable to find path through: " rpatterns)
                                            {:patterns rpatterns :bound bound :binding-outs binding-outs})))))))
                  ;; no patterns left, so add any remaining bindings
                  (order (vals binding-outs))))]
      (let [binding-outs (u/mapmap second identity eval-patterns)
            start (->> (if (seq prebound)
                         (filter (comp (partial some prebound) get-vars) patterns)
                         patterns)
                       (remove (comp (partial some binding-outs) get-vars))
                       (find-start pattern-counts))
            full-path (if start
                        (cons start (path-through (get-vars start) (without start patterns) binding-outs))
                        (path-through #{} patterns binding-outs))]
        (assert (= (count patterns) (count (remove eval-pattern? full-path)))
                (str "No valid paths through: " (vec patterns)))
        full-path))))

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
   opt-patterns
   not-patterns]
  (let [out-vars (fn [p] (if (eval-pattern? p) [(second p)] (get-vars p)))
        non-optional-vars (set (mapcat out-vars (concat planned-patterns general-patterns)))
        all-non-negation-vars (into non-optional-vars opt-patterns)
        filter-vars (u/mapmap get-vars (concat filter-patterns not-patterns))
        opt-vars (u/mapmap #(filter non-optional-vars (get-vars %)) opt-patterns)
        all-bound-for? (fn [bound fltr]
                         (every? #(or (bound %) (not (all-non-negation-vars %)))
                                 (filter-vars fltr)))
        plan-path-with-bound (fn [bound [op & patterns]]
                               (apply list op (plan-path graph patterns (assoc options :bound bound))))]
    (loop [plan []
           bound #{}
           [np & rp :as patterns] planned-patterns
           filters filter-patterns
           optionals opt-patterns
           negations not-patterns]
      (if-not (seq patterns)
        ;; no patterns left, so add remaining general patterns, negations, then filters
        (let [planned-optionals (map (partial plan-path-with-bound bound) optionals)
              planned-negations (map (partial plan-path-with-bound bound) negations)]
          (doall (concat plan general-patterns planned-optionals planned-negations filters)))

        ;; divide the filters into those which are fully bound, and the rest
        (let [all-bound? (partial all-bound-for? bound)
              all-non-opt-bound? (fn [p] (->> (opt-vars p)
                                              (remove bound)             ;; remove the bound ones
                                              empty?))                   ;; unbound ones left over
              nxt-filters (filter all-bound? filters)
              nxt-optionals (->> optionals
                                 (filter all-non-opt-bound?)
                                 (map (partial plan-path-with-bound bound)))
              nxt-negations (->> negations
                                 (filter all-bound?)
                                 (map (partial plan-path-with-bound bound)))
              negative-nexts (concat nxt-negations nxt-filters)
              remaining-optionals (remove all-non-opt-bound? optionals)
              remaining-filters (remove all-bound? filters)
              remaining-negations (remove all-bound? negations)]
          ;; if negatives were bound, append them, else get the next binding pattern
          (if (seq negative-nexts)
            (recur (into plan negative-nexts) bound patterns remaining-filters optionals remaining-negations)
            ;; if optionals were bound, append them, else get the next EPV Pattern
            (if (seq nxt-optionals)
              (recur (into plan nxt-optionals) (into bound (mapcat out-vars nxt-optionals)) patterns filters remaining-optionals negations)
              (recur (conj plan np) (into bound (out-vars np)) rp filters optionals negations))))))))

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
          [first-pattern] (or (and (seq bound)
                                   (seq (filter #(some bound (get-vars %)) independents)))
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
            all-ordered (concat ordered
                                (path bound group-countables count-map group-evals))]
        (if (empty? rmdr)
          (concat all-ordered (order evalps))
          (recur (first-group bound rmdr evalps) all-ordered))))))

(s/defn opt-type? :- s/Bool
  "Returns true if a pattern is a given operation type"
  [types :- #{s/Symbol}
   pattern :- PatternOrBindings]
  (and (seq? pattern) (contains? types (first pattern))))

(def not-operation? "Returns true if a pattern is a NOT operation" (partial opt-type? #{'not 'NOT}))

(def opt-operation? "Returns true if a pattern is a NOT operation" (partial opt-type? #{'optional 'OPTIONAL}))

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
                      (opt-operation? p) :opt-patterns  ;; (optional [?entity :attr "value"])
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
                eval-patterns not-patterns opt-patterns
                op-patterns unknown] :as p} (extract-patterns-by-type patterns)
        _ (when (seq unknown)
            (println "METAS: " (map meta patterns))
            (println "PATTERNS: " patterns)
            (throw (ex-info (str "Unknown form in query: " (str/join "," (doall unknown)))
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
    (merge-operations graph options planned planned-sub-patterns filter-patterns opt-patterns not-patterns)))

(s/defn new-or
  "Create an OR expression from a sequence of arguments.
   If an argument is a nested OR, then these terms are flattened into this level.
   If an argument is a NOT, then an exception is thrown."
  [terms]
  (if (= 1 count terms)
    (first terms)
    (->> terms
         (reduce
          (fn [acc [op & args :as term]]
            (if (seq? term)
              (case op
                or (into acc args)
                not (throw (ex-info "Illegal use of NOT expression in OR expression" {:or terms :not term}))
                (conj acc term))
              (conj acc term)))
          [])
         (list* 'or))))

(s/defn new-and
  "Create an AND expression from a sequence of arguments.
   If an argument is a nested AND, then these terms are flattened into this level."
  [terms]
  (if (= 1 (count terms))
    (first terms)
    (->> terms
         (reduce
          (fn [acc [op & args :as term]]
            (if (= 'and op)
              (into acc args)
              (conj acc term)))
          [])
         (list* 'and))))

(s/defn append
  "Appends a single element to the end of a seq"
  [s e]
  (if (vector? s) (conj s e) (concat s [e])))

(s/defn simplify-algebra :- [PatternOrBindings]
  "This operation simplifies the algebra into a sum-of-products form. "
  ([patterns :- [PatternOrBindings]] (simplify-algebra patterns {}))
  ([patterns :- [PatternOrBindings]
    options]
   (letfn [(or-term? [term] (= 'or (first term)))
           (sum-of-products
             ;; a term is either a single triple binding
             ;; or a list of (operator arg1 [arg2...])
             [term]
             (cond
               (vector? term) term
               (seq? term) (let [[op & args] term]
                             (case op
                               (not NOT)
                               (let [[op & args :as processed] (sum-of-products (list* 'and args))]
                                 (cond
                                   (vector? processed) (list 'not processed) ; single term, wrap as (not term)
                                   (vector? op) (list* 'not processed) ; multiple terms, wrap as (not t1 t2...)
                                   (= 'not op) (list* 'and args) ; nested not. Convert to (and t1 t2...)
                                   (= 'and op) (list* 'not args) ; and term, unwrap and just use (not t1 t2...)
                                   :default (list 'not processed))) ; other terms, just wrap in (not terms)

                               (optional OPTIONAL)
                               (let [[op & args :as processed] (sum-of-products (list* 'and args))]
                                 (cond
                                   (vector? processed) (list 'optional processed) ; single term, wrap as (optional term)
                                   (vector? op) (list* 'optional processed) ; multiple terms, wrap as (optional t1 t2...)
                                   (= 'and op) (list* 'optional args) ; and term, unwrap and just use (optional t1 t2...)
                                   :default (list 'optional processed)))

                               (or OR)
                               (new-or (map sum-of-products args))

                               (and AND)
                               (let [processed-args (doall (map sum-of-products args))
                                     or-terms (doall (filter or-term? processed-args))]
                                 (if (seq or-terms)
                                   (let [other-terms (remove or-term? processed-args)
                                         distribute-or (fn [acc [_ & args :as term]]
                                                         (concat acc
                                                                 (map #(new-and (append other-terms %)) args)))]
                                     (new-or (reduce distribute-or [] or-terms)))
                                   (new-and processed-args)))
                               (throw (ex-info (str "Unknown query operator: " op) {:op op :args args}))))
               :default (throw (ex-info (str "Unknown query term: " term) {:term term :type (type term)}))))]
     (let [[maybe-op & args :as result] (sum-of-products (list* 'and patterns))]
       (if (= 'and maybe-op)
         args
         (list result))))))

(s/defn normalize-sum-of-products
  "Converts an expression that is not a sum into a sum operation of one argument."
  [patterns]
  (if (and (seq? patterns) (#{'or 'OR} (first patterns)))
    patterns
    (list 'or
          (if (= 1 (count patterns))
            (first patterns)
            (list* 'and patterns)))))

(s/defn minimal-first-planner :- [PatternOrBindings]
  "Attempts to optimize a query, based on the principle that if smaller resolutions appear
   first in a product term, then lazy evaluation will lead to less iteration on the later terms.
   This is not always true, but is in the general case."
  [graph
   patterns :- [PatternOrBindings]
   options]
  (plan-path graph patterns options))

(s/defn user-plan :- [CountablePattern]
  "Returns the original order of patterns specified by the user. No optimization is attempted."
  [graph
   patterns :- [PatternOrBindings]
   options]
  patterns)

(def aggregate-types
  '#{max min count count-distinct sample
     sum avg median variance stddev})

(def wildcard? (partial = '*))

(def wildcard-permitted? '#{count sample count-distinct})

(defn aggregate-form?
  "Determines if a term is an aggregate. Also detects if a wildcard is used for anything that isn't count"
  [s]
  (or
   (and (vector? s)
        (some aggregate-form? s))
   (and (seq? s)
        (= 2 (count s))
        (let [fs (first s)]
          (and
           (aggregate-types fs)
           (if (and (wildcard? (second s)) (not (wildcard-permitted? fs)))
             (throw (ex-info (str "Wildcard is not permitted for " fs) {:form s}))
             true))))))

(def Aggregate (s/pred aggregate-form?))

(def VarOrWild (s/pred #(or (vartest? %) (wildcard? %))))

(s/defn aggregate-constraint :- (s/maybe Pattern)
  "Returns a constraint when it does or does not contains aggregates, selected by the aggregating? flag.
   For a compound constraint (and, or, not) then returns all non-empty elements
   that contain or do-not-contain aggregate vars."
  [aggregating? :- s/Bool
   needed-vars :- #{Var}
   aggregate-vars :- #{VarOrWild}
   constraint :- Pattern]
  (letfn [(agg-constraint [cnstrnt]
            (cond
              (vector? cnstrnt)
              (let [vars (get-vars cnstrnt)]
                (when (or
                       (and aggregating?
                            (or (some aggregate-vars vars)
                                (nil? (some needed-vars vars))))
                       (and (not aggregating?)
                            (some needed-vars vars)
                            ;; (or (nil? (some aggregate-vars vars)) (some needed-vars vars))
                            ))
                  cnstrnt))

              (seq? cnstrnt)
              (let [[op & args] cnstrnt
                    new-args (keep agg-constraint args)]
                (when (seq new-args)
                  (if (> (count new-args) 1)
                    (list* op new-args)
                    (first new-args))))

              :default (throw (ex-info (str "Unknown constraint structure: " cnstrnt)
                                       {:constraint cnstrnt}))))]
    (let [top-constraint (agg-constraint constraint)]
      (if (vector? top-constraint)
        (list 'and top-constraint)
        top-constraint))))

(def dot? (partial = '.))
(def Dot (s/pred dot?))
(def tdot? (partial = '...))
(def TDot (s/pred tdot?))

(def FindVectorElement
  (s/conditional
    tdot? TDot
    symbol? Var
    seq? Aggregate))

(def FindElement
  (s/conditional
    dot? Dot
    symbol? Var
    seq? Aggregate
    vector? [FindVectorElement]))

(s/defn split-aggregate-terms :- [(s/one [s/Any] "outer query constraints")
                                  (s/one [s/Any] "inner query constraints")
                                  (s/one #{VarOrWild} "vars to get aggregations for")]
  "Splits a WHERE clause up into the part suitable for an outer query,
   and the remaining constraints, which will be used for an inner query."
  ;; TODO: consider passing options, to select planning or not
  [constraints :- Pattern                    ;; the WHERE clause
   selection :- [FindElement]                ;; the FIND clause
   withs :- [Var]]                           ;; the WITH clause
  ;; extract the vars we know have to be in the outer query
  (let [[op & constaint-args] constraints
        _ (assert (= op 'or))
        vars (-> (filter vartest? selection) set (into withs))
        ;; extract the vars from the aggregation terms
        agg-vars (or (and (= 1 (count selection)) ;; the [expr ...] forms needs separate handling
                          (let [sel (first selection)]
                            (and
                             (vector? sel)
                             (if (and (= 2 (count sel)) (= '... (second sel)))
                               (-> sel first second hash-set)
                               (->> sel (filter aggregate-form?) (map second) set)))))
                     (->> selection (filter aggregate-form?) (map second) set))
        ;; remove the constraints containing aggregates
        non-agg-constraints (map (partial aggregate-constraint false vars agg-vars) constaint-args)
        agg-constraints (map (partial aggregate-constraint true vars agg-vars) constaint-args)]
    [non-agg-constraints agg-constraints agg-vars]))
