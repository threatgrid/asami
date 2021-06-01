(ns ^{:doc "Implements query operations based on data accessible through the Graph protocol."
      :author "Paula Gearon"}
    asami.query
    (:require [zuko.schema :refer [EPVPattern FilterPattern Pattern
                                   Results Value Var
                                   EvalPattern eval-pattern?
                                   epv-pattern? filter-pattern?
                                   op-pattern? vartest?]]
              [asami.planner :as planner :refer [Bindings PatternOrBindings Aggregate HasVars get-vars]]
              [asami.graph :as gr]
              [asami.internal :as internal]
              [zuko.sandbox :as sandbox]
              [zuko.projection :as projection]
              [zuko.util :refer [fn-for]]
              [zuko.logging :as log :include-macros true]
              [schema.core :as s :include-macros true]
              [clojure.set :as set]
              #?(:clj  [clojure.edn :as edn]
                 :cljs [cljs.reader :as edn])))

(def ^:dynamic *env* {})

(def ^:dynamic *select-distinct* distinct)

(def ^:dynamic *override-restrictions* false)

(def ^:const identity-binding (with-meta [[]] {:cols []}))

(def ^:const null-value nil)

(defn vars [s] (filter vartest? s))

(defn plain-var [v]
  (let [n (name v)]
    (if (#{\* \+} (last n))
      (symbol (namespace v) (subs n 0 (dec (count n))))
      v)))

(defn plain-vars [v] (map plain-var (vars v)))

(defn find-vars [f] (set (plain-vars f)))

(defn op-error
  [pattern]
  (throw (ex-info "Unknown operator" {:op (first pattern)
                                      :args (rest pattern)})))

(defn pattern-error
  [pattern]
  (throw (ex-info (str "Unknown pattern type in query: " pattern) {:pattern pattern})))

(declare operators operand-vars)

(extend-protocol HasVars
  #?(:clj Object :cljs object)
  (get-vars
    [pattern]
    (cond
      ;; bindings will pass the epv-pattern? test, so must check for this first
      (planner/bindings? pattern) (set (:cols (meta pattern)))
      (epv-pattern? pattern) (find-vars pattern)
      (filter-pattern? pattern) (or (:vars (meta pattern)) (find-vars (first pattern)))
      (op-pattern? pattern) (if (operators (first pattern))
                              (operand-vars pattern)
                              (op-error pattern))
      (eval-pattern? pattern) (let [[expr _] pattern]
                                (filter vartest? expr))
      :default (pattern-error pattern)))
  nil
  (get-vars [pattern] nil))

(defn operand-vars
  [o]
  (first
   (reduce (fn [[acc seen? :as s] v]
             (if (seen? v)
               s
               [(conj acc v) (conj seen? v)]))
           [[] #{}]
           (mapcat get-vars (rest o)))))

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
        total-cols (->> (plain-vars pattern)
                        (remove (set cols))
                        (concat cols)
                        (into []))
        pattern->left (projection/matching-vars pattern cols)]
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
        left->binding (projection/matching-vars lcols rcols)]
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

(s/defn missing :- (s/maybe Var)
  "Returns a value when it is a var that does not appear in the varmap."
  [varmap :- {Var s/Num}
   arg :- s/Any]
  (when (and (vartest? arg)
             (not (get varmap arg)))
    arg))

(def Fcn (s/pred #(or (fn? %) (var? %))))

(s/defn resolve-op :- (s/maybe Fcn)
  "Resolves a symbol to an associated function. Symbols without a namespace are presumed to be in clojure.core"
  [s :- s/Symbol]
  (when (or *override-restrictions*
            (if-let [n (namespace s)]
              (and (#{"clojure.core" "cljs.core" "clojure.string"} n)
                   (sandbox/allowed-fns (symbol (name s))))
              (sandbox/allowed-fns s)))
    (fn-for s)))

(s/defn retrieve-op :- Fcn
  "Retrieves a function for a provided operation. An op can be a variable, a function, a symbol for a function, or a string"
  [op var-map part]
  (or
   (cond
     (vartest? op) (retrieve-op (nth (first part) (var-map op)) var-map part) ;; assuming operation is constant
     (fn? op) op
     (symbol? op) (or (get *env* op) (resolve-op op))
     (string? op) (resolve-op (symbol op))
     :default (throw (ex-info (str "Unknown operation type" op) {:op op :type (type op)})))
   (throw (ex-info (str "Unsupported operation: " op) {:op op :type (type op)}))))

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
        arg-indexes (map-indexed
                     (fn [i arg]
                       (if-let [j (get var-map arg)]
                         j
                         (constantly (nth args i))))
                     args)
        filter-fn (if (vartest? op)
                    (if-let [op-idx (var-map op)]
                      (fn [a]
                        (let [callable-op (retrieve-op (nth a op-idx) var-map part)]
                          (apply callable-op (map (fn [f] (if (fn? f) (f) (nth a f))) arg-indexes))))
                      (throw (ex-info (str "Unknown variable: " op) {:op op})))
                    (let [callable-op (retrieve-op op var-map part)]
                      (fn [a]
                        (apply callable-op (map (fn [f] (if (fn? f) (f) (nth a f))) arg-indexes)))))]
    (try
      (with-meta (filter filter-fn part) m)
      (catch #?(:clj Throwable :cljs :default) e
        (throw (if-let [ev (some (partial missing var-map) args)]
                 (ex-info (str "Unknown variable in filter: " (name ev)) {:vars (keys var-map) :filter-var ev})
                 (ex-info (str "Error executing filter: " e) {:error e})))))))

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
                     (if-let [op-idx (var-map op)]
                       (fn [row]
                         (let [o (retrieve-op (nth row op-idx) var-map part)]
                           (concat row
                                   [(apply o
                                           (map
                                            #(if (neg? %) (nth expr (- %)) (nth row %))
                                            arg-indexes))])))
                       (throw (ex-info (str "Unknown variable: " op) {:op op})))
                     (let [callable-op (retrieve-op op var-map part)]
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
               pattern->left (projection/matching-vars fpattern cols)
               ;; find all bound vars that will be needed for the entire subquery
               vars (reduce #(into %1 (map plain-var (get-vars %2))) #{} patterns)
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
  "Iterates over the arguments to perform a left-join on each"
  [graph
   part :- Results
   [_ & patterns]]
  (reduce (fn [result pattern] (left-join pattern result graph)) part patterns))

(s/defn optional
  "Performs a left-outer-join, similarly to a conjunction"
  [graph
   part :- Results
   [_ & patterns :as operation]]
  (let [col-meta (meta part)
        cols (:cols col-meta)
        new-cols (->> (get-vars operation)
                      (remove (set cols)))
        empties (repeat (count new-cols) null-value)
        ljoin #(left-join %2 %1 graph)]
    (with-meta
      (mapcat (fn [lrow]
                (or (seq (reduce ljoin (with-meta [lrow] col-meta) patterns))
                    [(concat lrow empties)]))
              part)
      {:cols (vec (concat cols new-cols))})))

(def operators
  {'not {:left-join minus}
   'NOT {:left-join minus}
   'or {:left-join disjunction}
   'OR {:left-join disjunction}
   'and {:left-join conjunction}
   'AND {:left-join conjunction}
   'optional {:left-join optional}
   'OPTIONAL {:left-join optional}})

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
      {:cols (into namesl namesr)})))

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
          (with-meta values {:cols (vec a)})
          (throw (ex-info "Data does not match relation binding form" {:form nm :data values})))

        (and (symb? a) (= '... (first r)) (= 2 (count nm))) ; collection
        (if (coll? values)
          (with-meta (map vector values) {:cols [a]})
          (throw (ex-info "Tuples data does not match collection binding form" {:form nm :data values})))

        (and (every? symb? nm)) ; tuple
        (if (and (sequential? values) (= (count nm) (count values)))
          (with-meta [values] {:cols (vec nm)})
          (throw (ex-info "Tuples data does not match tuple binding form" {:form nm :data values})))

        :default (throw (ex-info "Unrecognized binding form" {:form nm}))))

    :default (ex-info "Illegal scalar binding form" {:form nm})))

(s/defn create-bindings :- [(s/one (s/maybe Bindings) "The bindings for other params")
                            (s/one (s/maybe s/Any) "The default graph")]
  "Converts user provided data for a query into bindings"
  [in :- [InSpec]
   values ;; :- (s/cond-pre (s/protocol Storage) s/Any)
   ]
  (if-not (seq in)
    [empty-bindings (first values)]
    (let [[default :as defaults] (remove nil? (map (fn [n v] (when (= '$ n) v)) in values))]
      (when (< 1 (count defaults))
        (throw (ex-info "Only one default data source permitted" {:defaults defaults})))
      (when-not (<= (count in) (count values))
        (throw (ex-info "In clause must not be more than the number of sources" {:in in :sources values})))
      (let [bindings (->> (map (fn [n v] (when-not (= '$ n) (create-binding n v))) in values)
                          (filter identity))
            bindings (when (seq bindings)
                       (reduce outer-product bindings))]
        [bindings default]))))

(defn conforms? [t d]
  (try
    (s/validate t d)
    (catch #?(:clj Exception :cljs :default) e (str ">>>" (.getMessage e) "<<<"))))

(s/defn select-planner
  "Selects a query planner function, based on user-selected options"
  [{:keys [planner] :as options}]
  (case planner
    :user planner/user-plan
    :min planner/plan-path
    planner/plan-path))

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
                                                                 {:cols (vec (plain-vars fpattern))})
                                                               patterns]
                                      ;; the first element is an operation,
                                      ;; start with an empty result and process all the patterns
                                      :default [identity-binding all-patterns])]
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

(def Plan {s/Keyword [s/Any]})

(def AuditableResults (s/if map? Plan Results))

(s/defn join-patterns :- AuditableResults
  "Joins the resolutions for a series of patterns into a single result.
   If options contains :path-plan then returns the plan and does not execute"
  [graph
   patterns :- [Pattern]
   bindings :- (s/maybe Bindings)
   {:keys [query-plan] :as options}]
  (let [all-patterns (if (seq bindings) (cons bindings patterns) patterns)
        path-planner (select-planner options)
        [fpath & rpath :as path] (path-planner graph all-patterns options)]
    (if query-plan
      {:plan path}
      (if-not rpath

        ;; single path element - executed separately as an optimization
        (cond
          (planner/bindings? fpath) fpath
          (epv-pattern? fpath) (with-meta
                                 (gr/resolve-pattern graph fpath)
                                 {:cols (vec (plain-vars fpath))})
          :default (run-simple-query graph [fpath]))

        ;; normal operation
        (let [ ;; if the plan begins with a negation, then it's not bound to the rest of
              ;; the plan, and it creates a "gate" for the result
              result-gate (gate-fn graph (take-while planner/not-operation? path))
              path' (drop-while planner/not-operation? path)]
          (-> (run-simple-query graph path')
              result-gate))))))

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
          (map? query) (update query :where seq)
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
  [selection constraints bindings graph project-fn {:keys [query-plan] :as options}]
  ;; joins must happen across a seq that is a conjunction
  (log/debug "executing selection: " (seq selection) " where: " constraints)
  (log/debug "bindings: " bindings)
  (let [top-conjunction (if (seq? constraints) ;; is this a list?
                          (let [[op & args] constraints]
                            (cond
                              (vector? op) constraints ;; Starts with top level EPV. Already in the correct form
                              (#{'and 'AND} op) args ;; Starts with top level AND. Use the arguments
                              (operators op) (list constraints) ;; top level form. Wrap as a conjunction
                              (seq? op) constraints ;; first form is an operation. Already in the correct form
                              :default (throw (ex-info "Unknown constraint format" {:constraint constraints}))))
                          (list constraints)) ;; a single vector, which is one constraint that needs to be wrapped. Unexpected
        select-distinct (fn [xs] (if (and (coll? xs) (not (vector? xs)))
                                   (let [m (meta xs)] (with-meta (*select-distinct* xs) m))
                                   xs))]
    (let [resolved (join-patterns graph top-conjunction bindings options)]
      (log/trace "results: " resolved)
      ;; check if this is a query plan without results
      (if query-plan
        resolved
        (->> resolved
             (project-fn selection)
             select-distinct)))))

(s/defn prepend
  [element
   pattern :- Pattern]
  (let [[op & args] pattern]
    (cond (vector? op) (cons element pattern))
          (#{'and 'AND} op) (cons element args)
          :default (list element pattern)))

(defn split-with*
  "Same as clojure.core/split-with but only executes the predicate once on each matching element.
  Returns the same as [(vec (take-while pred coll)) (drop-while pred coll)]"
  [pred coll]
  (loop [tw [] [r & rs :as allr] coll]
    (if-not (seq allr)
      [tw nil]       
      (if (pred r)            
        (recur (conj tw r) rs)
        [tw allr]))))

(defn seq-group-by
  "Does a group-by style of operation, but it streams over the input and provides a sequence of groups.
  The input data must already be in a groupable order, meaning that once a grouping variable has stopped
  appearing, the group is finished. group-select is a function for selecting the data to group by."
  [group-select xs]
  (letfn [(groups [[x & xs :as xa]]
            (when (seq xa)
              (let [g (group-select x)
                    gfn (fn [r] (= g (group-select r)))
                    [grp rmdr] (split-with* gfn xs)]
                (cons (cons x grp) (lazy-seq (groups rmdr))))))]
    (groups xs)))

(s/defn context-execute-query
  "For each line in a context, execute a query specified by the where clause"
  [graph
   grouping-vars :- #{Var}
   context :- Results
   [op & args :as where] :- Pattern]
  (let [context-cols (meta context)
        colnumbers (keep-indexed (fn [n col] (when (grouping-vars col) n)) (:cols context-cols))
        group-sel (fn [row] (mapv #(nth row %) colnumbers))
        ljoin #(left-join %2 %1 graph)
        where (if (#{'and 'AND} op) args (list where))
        subquery (fn [grp] (reduce ljoin grp where))]
    (->> (seq-group-by group-sel context)
         (map #(with-meta % context-cols))
         (map subquery))))

(def aggregate-fns
  "Map of aggregate symbols to functions that accept a seq of data to be aggregated"
  {'sum (partial apply +)
   'count count
   'count-distinct count  ;; removal of duplicates happens in processing the data
   'avg #(/ (apply + %) (count %))
   'max (partial apply max)
   'min (partial apply min)
   'first first
   'last last})

(s/defn agg-label :- s/Symbol
  "Converts an aggregate operation on a symbol into a symbol name"
  [[op v]]
  (symbol (str "?" (name op) "-" (if (planner/wildcard? v) "all" (subs (name v) 1)))))

(s/defn result-label :- s/Symbol
  "Convert an element from a select/find clause into an appropriate label.
   Note that duplicate columns are not considered"
  [e]
  (cond
    (vartest? e) e
    (and (seq? e) (= 2 (count e))) (agg-label e)
    :default (throw (ex-info "Bad selection in :find clause with aggregates" {:element e}))))

(defn distinct-fn
  "Returns a function that may deduplicate results, depending on the type of operator"
  [op]
  (if (= 'count-distinct op)
    identity
    *select-distinct*))

(s/defn aggregate-over :- s/Any  ;; Usually Results, but can be a seq or a value due to projection
  "For each seq of results, aggregates individually, and then together"
  [selection :- [s/Any]
   with :- [Var]
   partial-results :- [Results]]
  (let [var-index (fn [columns] (into {} (map-indexed (fn [n c] [c n]) columns)))
        selection-count (count selection)]
    (letfn [(var-index [columns] (into {} (map-indexed (fn [n c] [c n]) columns)))
            (get-selectors [idxs selected]
              (map (fn [s]
                     (if (vartest? s)
                       [first #(nth % (idxs s)) *select-distinct*]
                       (let [[op v] s
                             dedup-fn (distinct-fn op)]
                         (if-let [afn (aggregate-fns op)]
                           (if (planner/wildcard? v)
                             [afn identity dedup-fn]
                             [afn #(nth % (idxs v)) dedup-fn])
                           (throw (ex-info (str "Unknown operation: " op) {:expr s}))))))
                   selected))
            (project-aggregate [selected result]
              (let [idxs (var-index (:cols (meta result)))]
                (for [[col-fn col-selector dfn] (get-selectors idxs selected)]
                  (let [col-data (map col-selector result)]
                    (col-fn (dfn col-data))))))]
      (cond
        ;; check for singleton result
        (and (= 2 selection-count)
             (= '. (second selection)))
        (let [[op v :as sel] (first selection)
              result (first partial-results)
              col-fn (aggregate-fns op)
              dfn (distinct-fn op)]
          (if (planner/wildcard? v)
            (col-fn (dfn result))
            (let [idxs (var-index (:cols (meta result)))
                  col (idxs v)
                  col-data (map #(nth % col) result)]
              (col-fn (dfn col-data)))))

        ;; check for seq result
        (and (= 1 selection-count)
             (vector? (first selection)))
        ;; don't need to check if the first part of sel is an aggregate because we couldn't
        ;; be here if it weren't
        (let [selected (first selection)]
          (if (= '... (second selected))
            (let [[[op v :as sel] _] selected
                  col-fn (aggregate-fns op)
                  dfn (distinct-fn op)
                  single-agg (if (planner/wildcard? v)
                               (fn [result]
                                 (col-fn (dfn result)))
                               (fn [result]
                                 (let [col (get (var-index (:cols (meta result))) v)
                                       col-data (map #(nth % col) result)]
                                   (col-fn (dfn col-data)))))]
              (map single-agg partial-results))
            (project-aggregate (first selection) (first partial-results))))

        ;; standard :find clause with some of the selection as aggregates
        :default
        (with-meta
          (map (partial project-aggregate selection) partial-results)
          {:cols (mapv result-label selection)})))))

(defn aggregate-query
  [find bindings with where graph project-fn {:keys [query-plan] :as options}]
  (log/debug "aggregate query. plan only=" query-plan)
  (binding [*select-distinct* distinct]
    ;; flatten the query
    (let [simplified (planner/simplify-algebra where)
          ;; ensure that it is an (or ...) expression
          normalized (planner/normalize-sum-of-products simplified)
          ;; extract every element of the or into an outer/inner pair of queries.
          ;; The inner/outer -wheres zip
          [outer-wheres inner-wheres agg-vars] (planner/split-aggregate-terms normalized find with)
          _ (log/debug "inner query:" (seq inner-wheres))
          _ (log/debug "outer query:" (seq outer-wheres))
          ;; outer wheres is a series of queries that make a sum (an OR operation). These all get run separately.
          ;; inner wheres is a matching series of queries that get run for the corresponding outer query.

          ;; for each outer/inner pair, get all of the vars needed to be projected from the outer query
          ;; also need anything that joins the outer query to the inner query
          ;; start with the requested vars
          find-vars (filter vartest? find)
          ;; convert the requested vars into sets, for filtering
          find-var-set (set find-vars)
          with-set (set with)
          ;; create a function that can find everything in the outer query that the inner query needs
          ;; remove the columns which will already be projected from :find and :with
          needed-vars (fn [outer-where inner-where]
                        (let [inner-var-set (set (get-vars inner-where))]
                          (sequence (comp
                                     (remove find-var-set)
                                     (remove with-set)
                                     (remove agg-vars)
                                     (filter inner-var-set))
                                    (get-vars outer-where))))
          ;; execute the outer query if it exists. If not then return an identity binding.
          outer-results (map (fn [ow iw]
                               (if (seq ow)
                                 ;; outer query exists, so find the terms to be projected and execute
                                 (let [outer-terms (concat find-vars with (needed-vars ow iw))]
                                   (execute-query outer-terms ow bindings graph project-fn options))
                                 identity-binding))
                             outer-wheres inner-wheres)]
      (log/debug "outer results: " (into [] outer-results))
      (if query-plan
        ;; results are audit plans
        {:type :aggregate
         :outer-queries (map :plan outer-results)
         :inner-queries inner-wheres}
        ;; execute the inner queries within the context provided by the outer queries
        ;; remove the empty results. This means that empty values are not counted!
        (let [grouping-vars (into find-var-set with-set) ;; (set/difference (into find-var-set with-set) agg-vars)
              _ (log/debug "grouping vars: " grouping-vars)
              inner-results (filter seq (mapcat (partial context-execute-query graph grouping-vars) outer-results inner-wheres))]
          (log/debug "inner results: " (into [] inner-results))
          ;; calculate the aggregates from the final results and project
          (aggregate-over find with inner-results))))))


(defn ^:private fresh []
  (gensym "?__"))

(defn ^:private map-epv
  "In the :where sequence of query apply f to each EPV pattern."
  [f {:keys [where] :as query}]
  (assoc query
         :where (map (fn opf [constraint]
                       (cond
                         (epv-pattern? constraint)
                         (f constraint)

                         (op-pattern? constraint)
                         (cons (first constraint) (map opf (rest constraint)))

                         :else
                         constraint))
                     where)))

(s/defn rewrite-wildcards
  "In the :where sequence of query replace all occurrences of _ with
  unique variables."
  [{:keys [where] :as query}]
  (map-epv (fn [epv]
             (mapv (fn [x]
                     (if (= x '_) (fresh) x))
                   epv))
           query))

(s/defn expand-shortened-pattern-constraints
  "In the :where sequence of query expand EPV patterns of the form [E]
  and [E P] to [E ?P ?V] and [E ?P ?V] respectively where ?P and ?V
  are fresh variables."
  [{:keys [where] :as query}]
  (map-epv (fn [epv]
             (into epv (repeatedly (- 3 (count epv)) fresh)))
           query))

(s/defn parse
  [x]
  (-> x
      query-map
      query-validator
      rewrite-wildcards
      expand-shortened-pattern-constraints))

(s/defn query-entry
  "Main entry point of user queries"
  [query empty-graph inputs plan?]
  (let [{:keys [find all in with where]} (parse query)
        [inputs options] (if (seq in)
                           [(take (count in) inputs) (drop (count in) inputs)]
                           [[(first inputs)] (rest inputs)])
        options (-> (apply hash-map options) (assoc :query-plan plan?))
        [bindings default-graph] (create-bindings in inputs)
        graph (or default-graph empty-graph)
        project-fn (partial projection/project internal/project-args)]
    (if (seq (filter planner/aggregate-form? find))
      (aggregate-query find bindings with where graph project-fn options)
      (binding [*select-distinct* (if all identity distinct)]
        (execute-query find where bindings graph project-fn options)))))
