(ns ^{:doc "Implements a full query engine based on fully indexed data."
      :author "Paula Gearon"}
    asami.query
    (:require [naga.schema.store-structs :as st
                                         :refer [EPVPattern FilterPattern Pattern
                                                 Results Value Var
                                                 EvalPattern eval-pattern?
                                                 epv-pattern? filter-pattern?
                                                 op-pattern? vartest?]]
              [naga.store :refer [Storage StorageType]]
              [naga.storage.store-util :as store-util]
              [asami.planner :as planner :refer [Bindings PatternOrBindings HasVars get-vars]]
              [asami.graph :as gr]
              [naga.util :refer [fn-for]]
              #?(:clj  [schema.core :as s]
                 :cljs [schema.core :as s :include-macros true])
              #?(:clj  [clojure.edn :as edn]
                 :cljs [cljs.reader :as edn])))

(def ^:dynamic *select-distinct* distinct)

(s/defn find-vars [f] (set (filter st/vartest? f)))

(defn op-error
  [pattern]
  (throw (ex-info "Unknown operator" {:op (first pattern)
                                      :args (rest pattern)})))

(defn pattern-error
  [pattern]
  (throw (ex-info (str "Unknown pattern type in query: " pattern) {:pattern pattern})))

(declare operators)

(extend-type #?(:clj Object :cljs object)
  HasVars
  (get-vars
    [pattern]
    (cond
      ;; bindings will pass the epv-pattern? test, so must check for this first
      (planner/bindings? pattern) (set (:cols (meta pattern)))
      (epv-pattern? pattern) (set (st/vars pattern))
      (filter-pattern? pattern) (or (:vars (meta pattern)) (find-vars (first pattern)))
      (op-pattern? pattern) (if-let [{:keys [get-vars]} (operators (first pattern))]
                              (get-vars pattern)
                              (op-error pattern))
      (eval-pattern? pattern) (let [[expr _] pattern]
                                (filter vartest? expr))
      :default (pattern-error pattern))))

(s/defn without :- [s/Any]
  "Returns a sequence minus a specific element"
  [e :- s/Any
   s :- [s/Any]]
  (remove (partial = e) s))


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
  "Takes a partial result, and does an inner join against the resolution of a pattern.
   Iterates over the partial result, using the bindings to update the pattern to search the index.
   Each row in the partial result is then repeated to match the rows returned from the index lookup.
   If no variables match, then the result will be an outer product of the partial result, and
   the rows returned from an index lookup of the unmodified pattern.
   The final result has metadata with the columns from the partial result,
   and all new vars from the pattern."
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
            rrow (gr/resolve-pattern graph lookup)]
        (concat lrow rrow))
      {:cols total-cols})))

(defn vconj
  "Used to update a value to be a vector with the new element conj'ed.
   If the value starts as nil, then create a new vector to hold the element."
  [c v]
  (if c (conj c v) [v]))

(s/defn prebound-left-join :- Results
  "Takes a bindings (Results) and joins on the current results. This is similar to the
   pattern-left-join but instead of taking a pattern to be applied to an indexed graph,
   it takes unindexed data that is already bound. The join is done by indexing the bindings
   and using the same algorithm that joins rows in pattern-left-join"
  [part :- Results
   bindings :- Results]
  (let [lcols (:cols (meta part))
        rcols (:cols (meta bindings))
        total-cols (->> rcols
                        (remove (set lcols))
                        (concat lcols)
                        (into []))
        left->binding (store-util/matching-vars lcols rcols)]
    (with-meta
      (if (seq left->binding)
        (let [key-indices (sort (keys left->binding))
              select-key (fn [row] (map #(nth row %) key-indices))
              val-set (set (vals left->binding))
              width (count rcols)
              split-row (fn [row] (reduce (fn [[k v] n]
                                            (let [rowval (nth row n)]
                                              (if (val-set n)
                                                [(conj k (nth row n)) v]
                                                [k (conj v (nth row n))])))
                                          [[] []]
                                          (range width)))
              local-index (reduce (fn [idx row]
                                    (let [[shared new] (split-row row)]
                                      (update idx shared vconj new)))
                                  {} bindings)]
          (for [lrow part
                rrow (local-index (select-key lrow))]
            (concat lrow rrow)))
        (for [row-l part row-r bindings]
          (concat row-l row-r)))
      {:cols total-cols})))

(s/defn filter-join
  "Uses row bindings in a partial result as arguments to a function whose parameters are defined by those names.
   Those rows whose bindings return true/truthy are kept and the remainder are removed."
  [graph
   part :- Results
   [[op & args :as fltr]] :- FilterPattern]
  (let [m (meta part)
        var-map (->> (:cols m)
                     (map-indexed (fn [a b] [b a]))
                     (into {}))
        arg-indexes (map-indexed #(var-map %2 (- %1)) args)
        callable-op (cond (fn? op) op
                          (symbol? op) (fn-for op)
                          (string? op) (fn-for (symbol op))
                          :default (throw (ex-info (str "Unknown filter operation type" op) {:op op :type (type op)})))
        filter-fn (fn [& [a]]
                    (apply callable-op (map #(if (neg? %) (nth args (- %)) (nth a %)) arg-indexes)))]
    (with-meta (filter filter-fn part) m)))

(s/defn binding-join
  "Uses row bindings as arguments for an expression that uses the names in that binding.
   Binds a new var to the result of the expression and adds it to the complete results."
  [graph
   part :- Results
   [[op & args :as expr] bnd-var] :- EvalPattern]
  (let [cols (vec (:cols (meta part)))
        new-cols (conj cols bnd-var)
        var-map (->> cols
                     (map-indexed (fn [a b] [b a]))
                     (into {}))
        arg-indexes (keep-indexed #(when-not (zero? %1) (var-map %2 (- %1))) expr)
        expr (vec expr)
        binding-fn (if (vartest? op)
                     (let [op-idx (var-map op)]
                       (fn [row]
                         (concat row
                                 [(apply (nth row op-idx)
                                         (map
                                          #(if (neg? %) (nth expr (- %)) (nth row %))
                                          arg-indexes))])))
                     (let [callable-op (cond (fn? op) op
                                             (symbol? op) (fn-for op)
                                             (string? op) (fn-for (symbol op)))]
                       (fn [row]
                         (concat row
                                 [(apply callable-op
                                         (map
                                          #(if (neg? %) (nth expr (- %)) (nth row %))
                                          arg-indexes))]))))]
    (with-meta
      (map binding-fn part)
      {:cols new-cols})))


(def ^:dynamic *plan-options* [:min])

(declare left-join)

(s/defn minus
  "Removes matches.
   For each line in the current results, performs a subquery with NOT clause.
   When the NOT clause returns data then that line in the results should be removed."
  [graph
   part :- Results
   [_ & [fpattern :as patterns]]]  ;; 'not symbol, then the pattern arguments
  (let [ljoin #(left-join %2 %1 graph)
        col-meta (meta part)]
    (with-meta
      (remove
       (if (epv-pattern? fpattern)  ;; this test is an optimization, to avoid matching-vars in a tight loop
         (let [cols (:cols col-meta)  ;; existing bound column names
               ;; map the first pattern's vars into the existing binding columns. Used for the first resolution.
               pattern->left (store-util/matching-vars fpattern cols)
               ;; find all bound vars that will be needed for the entire subquery
               vars (reduce #(into %1 (get-vars %2)) #{} patterns)
               ;; get the required bound column names, in order
               pre-bound (keep vars cols)
               ;; the required bound column indexes
               pattern-val-idxs (set (keep-indexed (fn [n col] (when (vars col) n)) cols))
               ;; the columns about to get bound from the first pattern of the subquery
               un-bound (keep-indexed #(when (and (vartest? %2) (not (pattern->left %1))) %2) fpattern)
               ;; all the columns resulting from resolving the first pattern of the subquery
               first-cols {:cols (vec (concat pre-bound un-bound))}]
           (fn [part-line]
             ;; update the first pattern of the subquery to include the current bindings
             (let [lookup (modify-pattern part-line pattern->left fpattern)
                   ;; start the bindings with the known bound values. Based on column number, not string comparison
                   bound-cols (vec (keep-indexed #(when (pattern-val-idxs %1) %2) part-line))]
               ;; Perform the subquery.
               ;; Start by resolving the first pattern with the contextual bindings
               ;; and then left-join through the rest of the subquery.
               ;; seq returns truthy when results are found, and falsey when there are no results.
               (seq
                (reduce ljoin
                        (with-meta
                          (map (partial into bound-cols) (gr/resolve-pattern graph lookup))
                          first-cols)
                        (rest patterns))))))
         ;; general subquery operation when the first element is not a graph resolution pattern
         (fn [part-line]
           (seq (reduce ljoin (with-meta [part-line] col-meta) patterns))))
       part)
      col-meta)))

(s/defn disjunction
  "Implements an OR operation by repeating a join across each arm of the operation,
   and concatenating the results"
  [graph
   part :- Results
   [_ & patterns]]  ;; Discard the first element, since it is just the OR operator
  (let [spread (map #(left-join % part graph) patterns)
        cols (:cols (meta (first spread)))]
    (doseq [s (rest spread)]
      (when (not= (:cols (meta s)) cols)
        (throw (ex-info
                "Alternate sides of OR clauses may not contain different vars"
                (zipmap patterns (map (comp :cols meta) spread))))))
    (with-meta
      ;; Does distinct create a scaling issue?
      (*select-distinct* (apply concat (map #(left-join % part graph) patterns)))
      {:cols (:cols (meta (first spread)))})))

(s/defn conjunction
  "Reorders arguments for left-join and drop the AND operator"
  [graph
   part :- Results
   [_ & patterns]]
  (left-join patterns part graph))

(def operators
  {'not {:get-vars #(mapcat get-vars (rest %))
         :left-join minus}
   'or {:get-vars #(mapcat get-vars (rest %))
        :left-join disjunction}
   'and {:get-vars #(mapcat get-vars (rest %))
         :left-join conjunction}})

(defn left-join
  "Joins a partial result (on the left) to a pattern (on the right).
   The pattern type will determine dispatch."
  [pattern results graph]
  (cond
    ;; bindings will pass the epv-pattern? test, so must check for this first
    (planner/bindings? pattern) (prebound-left-join results pattern)
    (epv-pattern? pattern) (pattern-left-join graph results pattern)
    (filter-pattern? pattern) (filter-join graph results pattern)
    (eval-pattern? pattern) (binding-join graph results pattern)
    (op-pattern? pattern) (if-let [{:keys [left-join]} (operators (first pattern))]
                            (left-join graph results pattern)
                            (op-error pattern))
    :default (pattern-error pattern)))

(s/def InSpec (s/conditional #(= '$ %) s/Symbol
                             #(and (symbol? %) (= \? (first (name %)))) s/Symbol
                             #(and sequential? (sequential? (first %))) [[s/Symbol]]
                             :else [s/Symbol]))

(s/defn outer-product :- Bindings
  "Creates an outer product between 2 sets of bindings"
  [leftb :- Bindings
   rightb :- Bindings]
  (let [namesl (:cols (meta leftb))
        namesr (:cols (meta rightb))]
    (when-let [n (seq (filter (set namesl) namesr))]
      (throw (ex-info "Outer product between bindings should have distinct names" {:duplicate-names n})))
    (with-meta
      (for [row-l leftb row-r rightb]
        (concat row-l row-r))
      {:cols (concat namesl namesr)})))

(s/defn symb?
  "Similar to symbol? but excludes the special ... form"
  [s]
  (and (symbol? s) (not= s '...)))

(def empty-bindings (with-meta [] {:cols []}))

(s/defn create-binding :- Bindings
  "Creates a bindings between a name and a set of values.
   If the name is singular, then that name is bound to the singular element found in values.
   If the name is a seq, then each name in the seq is bound to the corresponding offset in each value."
  [nm :- InSpec values]
  (cond
    (symbol? nm) (with-meta [[values]] {:cols [nm]})
    (sequential? nm)
    (let [[a & r] nm]
      (cond
        (and (sequential? a) (nil? r) (every? symb? a)) ; relation
        (if (every? #(= (count a) (count %)) values)
          (with-meta values {:cols a})
          (throw (ex-info "Data does not match relation binding form" {:form nm :data values})))

        (and (symb? a) (= '... (first r)) (= 2 (count nm))) ; collection
        (if (sequential? values)
          (with-meta (map vector values) {:cols [a]})
          (throw (ex-info "Tuples data does not match collection binding form" {:form nm :data values})))

        (and (every? symb? nm)) ; tuple
        (if (and (sequential? values) (= (count nm) (count values)))
          (with-meta [values] {:cols nm})
          (throw (ex-info "Tuples data does not match tuple binding form" {:form nm :data values})))

        :default (throw (ex-info "Unrecognized binding form" {:form nm}))))

    :default (ex-info "Illegal scalar binding form" {:form nm})))

(s/defn create-bindings :- [(s/one (s/maybe Bindings) "The bindings for other params")
                            (s/one (s/maybe StorageType) "The default storage")]
  "Converts user provided data for a query into bindings"
  [in :- [InSpec]
   values :- (s/cond-pre (s/protocol Storage) s/Any)]
  (if-not (seq in)
    [empty-bindings (first values)]
    (let [[default :as defaults] (filter identity (map (fn [n v] (when (= '$ n) v)) in values))]
      (when (< 1 (count defaults))
        (throw (ex-info "Only one default data source permitted" {:defaults defaults})))
      (when-not (<= (count in) (count values))
        (throw (ex-info "In clause must not be more than the number of sources" {:in in :sources values})))
      [(->> (map (fn [n v] (when-not (= '$ n) [n v])) in values)
            (filter identity)
            (map (partial apply create-binding))
            (reduce outer-product))
       default])))

(defn conforms? [t d]
  (try
    (s/validate t d)
    (catch #?(:clj Exception :cljs :default) e (str ">>>" (.getMessage e) "<<<"))))

(s/defn select-planner
  "Selects a query planner function, based on user-selected options"
  [options]
  (let [opt (set options)]
    (case (get opt :planner)
      :user planner/user-plan
      :min planner/plan-path  ; TODO: switch to minimal-first-planner
      planner/plan-path)))

(s/defn run-simple-query
  [graph
   [fpattern & patterns :as all-patterns] :- [PatternOrBindings]]
  ;; if provided with bindings, then join the entire path to them,
  ;; otherwise, start with the first item in the path, and join the remainder
  (let [ ;; execute the plan by joining left-to-right
        ;; left-join has back-to-front params for dispatch reasons
        ljoin #(left-join %2 %1 graph)
        ;; resolve the initial part of the query, and get the remaining patterns to join
        [part-result proc-patterns] (cond
                                      ;; the first element is already a partial result
                                      (planner/bindings? fpattern) [fpattern patterns]
                                      ;; the first element is a pattern lookup
                                      (epv-pattern? fpattern) [(with-meta
                                                                 (gr/resolve-pattern graph fpattern)
                                                                 {:cols (st/vars fpattern)})
                                                               patterns]
                                      ;; the first element is an operation,
                                      ;; start with an empty result and process all the patterns
                                      :default [(with-meta [[]] {:cols []}) all-patterns])]
    ;; process the remaining query
    (reduce ljoin part-result proc-patterns)))

(s/defn gate-fn
  "Returns a function that allows data through it or not,
   based on the results of a series of NOT operations.
   If any operation returns results, then nothing may get through."
  [graph
   constraints :- [Pattern]]
  (if-not (seq constraints)
    identity
    (loop [[[_ & patterns :as constraint] & remaining] constraints]
      (if-not constraint
        identity
        (if (seq (run-simple-query graph patterns))
          (constantly [])
          (recur remaining))))))

(s/defn join-patterns :- Results
  "Joins the resolutions for a series of patterns into a single result."
  [graph
   patterns :- [Pattern]
   bindings :- (s/maybe Bindings)
   & options]
  (let [all-patterns (if (seq bindings) (cons bindings patterns) patterns)
        path-planner (select-planner options)
        [fpath & rpath :as path] (path-planner graph all-patterns options)]
    (if-not rpath

      ;; single path element - executed separately as an optimization
      (cond
        (planner/bindings? fpath) fpath
        (epv-pattern? fpath) (with-meta
                               (gr/resolve-pattern graph fpath)
                               {:cols (st/vars fpath)})
        :default (run-simple-query graph [fpath]))

      ;; normal operation
      (let [ ;; if the plan begins with a negation, then it's not bound to the rest of
            ;; the plan, and it creates a "gate" for the result
            result-gate (gate-fn graph (take-while planner/not-operation? path))
            path' (drop-while planner/not-operation? path)]
        (-> (run-simple-query graph path')
            result-gate)))))

(s/defn add-to-graph
  [graph
   data :- Results]
  (reduce (fn [acc d] (apply gr/graph-add acc d)) graph data))

(s/defn delete-from-graph
  [graph
   data :- Results]
  (reduce (fn [acc d] (apply gr/graph-delete acc d)) graph data))

(def query-keys #{:find :in :with :where})

(s/defn query-map
  "Parses a query into it's main components.
   Queries can be a sequence, a map, or an EDN string. These are based on Datomic-style queries.
   The return map contains:
   :find - The elements to be projected from a query.
   :all - true if duplicates are to be returned, false otherwise. Defaults to false.
   :in - A list of data sources. Optional.
   :with - list of variables for grouping.
   :where - A sequence of constraints for the query."
  [query]
  (let [{find :find :as qmap}
        (cond
          (map? query) query
          (string? query) (query-map (edn/read-string query))
          (sequential? query) (->> query
                                   (partition-by query-keys)
                                   (partition 2)
                                   (map (fn [[[k] v]] [k v]))
                                   (into {})))
        [find' all] (if (= :all (first find))
                      [(rest find) true]
                      [(remove #{:distinct} find) false])]
    (assoc qmap :find find' :all all)))

(s/defn newl :- s/Str
  [s :- (s/maybe s/Str)
   & remaining]
  (if s (apply str s "\n" remaining) (apply str remaining)))

(s/defn query-validator
  [{:keys [find in with where] :as query} :- {s/Keyword (s/cond-pre s/Bool [s/Any])}]
  (let [extended-query-keys (into query-keys [:all :distinct])
        unknown-keys (->> (keys query) (remove (conj extended-query-keys)) seq) 
        non-seq-wheres (seq (remove sequential? where))
        err-text (cond-> nil
                   unknown-keys (newl "Unknown clauses: " unknown-keys)
                   (empty? find) (newl "Missing ':find' clause")
                   (empty? where) (newl "Missing ':where' clause")
                   non-seq-wheres (newl "Invalid ':where' statements: " non-seq-wheres))]
    (if err-text
      (throw (ex-info err-text {:query query}))
      query)))

(s/defn execute-query
  [selection constraints bindings graph store]
  (->> (join-patterns graph constraints bindings)
       (store-util/project store selection)
       *select-distinct*))

(def aggregate-types
  '#{max min count count-distinct sample
     sum avg median variance stddev})

(defn aggregate-form?
  "Determines if a term is an aggregate"
  [s]
  (and (list? s)
       (= 2 (count s))
       (aggregate-types (first s))))

(def Aggregate (s/pred aggregate-form?))

(s/defn split-aggregate-terms :- [(s/one [s/Any] "outer query constraints")
                                  (s/one [s/Any] "outer query constraints")]
  "Splits a WHERE clause up into the part suitable for an outer query,
   and the remaining constraints, which will be used for an inner query."
  [constraints :- [Pattern]
   aggregates :- [Aggregate]]
  )

(s/defn aggregate-over :- Results
  "For each row in the partial results, executes an inner query, and aggregates the results"
  [aggregates :- [Aggregate]
   with-terms :- [Var]
   agg-constraints :- [Pattern]
   partial-results :- Results]
  )

(s/defn query-entry
  "Main entry point of user queries"
  [query empty-store empty-graph inputs]
  (let [{:keys [find all in with where]} (-> (query-map query)
                                             query-validator)
        [bindings default-store] (create-bindings in inputs)
        store (or default-store empty-store)
        graph (or (:graph store) empty-graph)]
    (if-let [aggregates (seq (filter aggregate-form? find))]
      (binding [*select-distinct* distinct]
        (let [[outer-where inner-where] (split-aggregate-terms where aggregates)
              outer-terms (remove (set aggregates) find)
              outer-result (execute-query outer-terms outer-where bindings graph store)]
          (aggregate-over aggregates with inner-where outer-result)))
      (binding [*select-distinct* (if all identity distinct)]
        (execute-query find where bindings graph store)))))
