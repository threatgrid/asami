(ns ^{:doc "Projection functions"
      :author "Paula Gearon"}
    asami.projection
  #?(:cljs (:refer-clojure :exclude [Var]))
  (:require [schema.core :as s]
            #?(:cljs [cljs.core :refer [Symbol]]))
  #?(:clj (:import [clojure.lang Symbol])))

;; single element in a rule
(def EntityPropertyElt s/Any)

;; simple pattern containing a single element. e.g. [?v]
(def EntityPattern [(s/one s/Symbol "entity")])

;; two or three element pattern.
;; e.g. [?s :property]
;;      [:my/id ?property ?value]
(def EntityPropertyPattern
  [(s/one EntityPropertyElt "entity")
   (s/one EntityPropertyElt "property")
   (s/optional s/Any "value")])

;; The full pattern definition, with 1, 2 or 3 elements
(def EPVPattern
  (s/if #(= 1 (count %))
    EntityPattern
    EntityPropertyPattern))

;; Less restrictive than EPVPattern, because this is called at runtime
(s/defn epv-pattern? :- s/Bool
  [pattern :- [s/Any]]
  (and (vector? pattern)
       (let [f (first pattern)]
         (and (boolean f) (not (seq? f))))))

(def Value s/Any)

(def Results [[Value]])

(def EntityPropAxiomElt
  (s/cond-pre s/Keyword #?(:clj Long :cljs s/Num)))

(def EntityPropValAxiomElt
  (s/conditional (complement symbol?) s/Any))

(def Triple
  [(s/one s/Any "entity")
   (s/one s/Any "property")
   (s/one s/Any "value")])

(def Axiom
  [(s/one EntityPropAxiomElt "entity")
   (s/one EntityPropAxiomElt "property")
   (s/one EntityPropValAxiomElt "value")])

(def Var (s/constrained s/Symbol (comp #{\? \%} first name)))

(s/defn vartest? :- s/Bool
  [x]
  (and (symbol? x) (boolean (#{\? \%} (first (name x))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(s/defn project-row :- [s/Any]
  "Creates a new EPVPattern from an existing one, based on existing bindings.
   Uses the mapping to copy from columns in 'row' to overwrite variables in 'pattern'.
   'pattern' must be a vector.
   The index mappings have already been found and are in the 'mapping' argument"
  [{:keys [new-node node-label] :as store-fns}
   wide-pattern :- [s/Any]
   nodes :- (s/maybe [s/Num])
   mapping :- {s/Num s/Num}
   row :- [Value]]
  (let [get-node (memoize (fn [_] (new-node)))
        node-statements (mapcat (fn [i]
                                  (let [node (get-node i)]
                                    [node :db/ident (node-label node)]))
                                nodes)
        update-pattern (fn [p [t f]]
                         (let [v (if (neg? f) (get-node f) (nth row f))]
                           (assoc p t v)))]
    (vec
      (concat node-statements
        (reduce update-pattern wide-pattern mapping)))))

(s/defn matching-vars :- {s/Num s/Num}
  "Returns pairs of indexes into seqs where the vars match.
   For any variable that appears in both sequences, the column number in the
   'from' parameter gets mapped to the column number of the same variable
   in the 'to' parameter."
  [from :- [s/Any]
   to :- [Symbol]]
  (->> to
       (keep-indexed
        (fn [nt vt]
          (seq
           (keep-indexed
            (fn [nf vf]
              (if (and (vartest? vf) (= vt vf))
                [nf nt]))
            from))))
       (apply concat)
       (into {})))

(s/defn offset-mappings :- {s/Num s/Num}
  "Build a pattern->data mapping that returns offsets into a pattern mapped to corresponding
   offsets into data. If a data offset is negative, then this indicates a node must be built
   instead of reading from the data."
  [full-pattern :- [s/Any]
   data-vars :- [Symbol]
   data :- Results]
  (let [known-vars (set data-vars)
        var-positions (matching-vars full-pattern data-vars)
        fresh-map (->> full-pattern
                       (filter #(and (vartest? %) (not (known-vars %))))
                       set
                       (map-indexed (fn [n v] [v (- (inc n))]))
                       (into {}))]
    (->> full-pattern
         (map-indexed
          (fn [n v] (if (and (nil? (var-positions n)) (vartest? v)) [n (fresh-map v)])))
         (filter identity)
         (into var-positions))))

(s/defn dedupe-by :- Results
  "Removes duplicate rows, only considering the columns of the specified data for projection.
  This means the column keys for which the values are non-negative."
  [pattern->data :- {s/Num s/Num}
   data :- Results]
  (let [cols (filter #(>= % 0) (vals pattern->data))
        row-data (fn [r] (mapv (partial nth r) cols))
        td (fn [rf]
             (let [pvs (volatile! #{})]
               (fn
                 ([] (rf))
                 ([result] (rf result))
                 ([result input]
                  (let [row (row-data input)]
                    (if (@pvs row)
                      result
                      (do
                        (vswap! pvs conj row)
                        (rf result input))))))))]
    (sequence td data)))

(s/defn new-nodes :- [s/Num]
  "Returns all the new node references that appears in a map of offsets.
   Node references are negative numbers."
  [offset-map :- {s/Num s/Num}]
  (seq (set (filter neg? (vals offset-map)))))

(s/defn group-exists? :- [s/Any]
  "Determines if a group is instantiating a new piece of data,
   and if so checks if it already exists."
  [resolve-pattern-fn
   group :- [Axiom]]
  (if-let [[entity _ val :as g] (some (fn [[_ a _ :as axiom]] (when (= a :db/ident) axiom)) group)]
    (seq (resolve-pattern-fn ['?e :db/ident val]))))

(s/defn adorn-entities :- [Axiom]
  "Marks new entities as Naga entities"
  [triples :- [Axiom]]
  (reduce (fn [acc [e a v :as triple]]
            (let [r (conj acc triple)]
              (if (= :db/ident a) (conj r [e :tg/entity true]) r)))
          []
          triples))

(s/defn project-single :- s/Any
  "Returns a single value out of the first row of results, selected by the variable.
  Throws an exception if the variable does not exist."
  [v :- Var
   columns :- [Var]
   data :- Results]
  (if-let [col (first (keep-indexed (fn [n c] (when (= v c) n)) columns))]
    (nth (first data) col)
    (throw (ex-info (str "Projection variable was not in the selected data: " v) {:var v :data columns}))))

(s/defn project-collection :- [s/Any]
  "Returns a single value from every row of results, selected by the variable."
  [v :- Var
   columns :- [Var]
   data :- Results]
  (if-let [col (first (keep-indexed (fn [n c] (when (= v c) n)) columns))]
    (map #(nth % col) data)
    (throw (ex-info (str "Projection variable was not in the selected data: " v) {:var v :data columns}))))

(s/defn project-tuple :- [s/Any]
  "Returns a tuple of values out of the first row of results, selected by the variables.
  Throws an exception if any of the variables do not exist."
  [tuple :- [Var]
   columns :- [Var]
   data :- Results]
  (when (seq data)
    (let [width (count tuple)
          col-mapping (matching-vars tuple columns)
          row (first data)]
      (if (= width (count col-mapping))
        (with-meta (mapv #(nth row (col-mapping %)) (range width)) {:cols tuple})
        (let [missing (->> (range (count tuple))
                           (remove col-mapping)
                           (mapv (partial nth tuple)))]
          (throw (ex-info (str "Projection variables not found in the selected data: " missing)
                          {:missing missing :data columns})))))))

(s/defn derive-pattern-and-columns :- [[s/Any] [s/Any]]
  "Given a pattern (possibly with usage of the :as syntax), return a
  pair of a pattern without usage of the :as syntax and renamed
  columns as described by the :as syntax.

  Example:

    (derive-pattern-and-columns '[?x :data :as ?z ?y])
    ;; => [[?x :data ?y] [?x ?z ?y]]"
  [pattern]
  (loop [pattern pattern
         new-pattern []
         renamed-columns []]
    (if (seq pattern)
      (let [[x y z :as xs] (take 3 pattern)
            new-pattern (conj new-pattern x)]
        (if (= :as y)
          (recur (drop 3 pattern)
                 new-pattern
                 (conj renamed-columns (or z x)))
          (recur (rest pattern)
                 new-pattern
                 (conj renamed-columns x))))
      [new-pattern renamed-columns])))

(s/defn project-results :- Results
  "Converts each row from a result, into just the requested columns, as per the patterns arg.
   Any specified value in the patterns will be copied into that position in the projection.
   Unbound patterns will generate new nodes for each row.
  e.g. For pattern [?h1 :friend ?h2]
       data: [[h1=frodo h3=bilbo h2=gandalf]
              [h1=merry h3=pippin h2=frodo]]
  leads to: [[h1=frodo :friend h2=gandalf]
             [h1=merry :friend h2=frodo]]"
  [{:keys [new-node node-label] :as store-fns}
   pattern :- [s/Any]
   columns :- [Var]
   data :- Results]
  (let [[full-pattern pattern-columns] (derive-pattern-and-columns pattern)
        pattern->data (offset-mappings full-pattern columns data)
        nodes (new-nodes pattern->data)]
    (with-meta
      (map #(project-row store-fns full-pattern nodes pattern->data %) data)
      {:cols pattern-columns})))

(s/defn project :- s/Any
  "Converts data from results, into just the requested columns, as per the patterns arg.
   Depending on the format of the `pattern` argument, different projections may occur.
   - If the pattern is a sequence of vars, then the associated column in each row of data
   will be copied into position in each row of results.
   - If the pattern is a var followed by a dot, then only the first row of data is processed
   and a single value matching the var will be returned.
   - If the pattern is a vector containing a var and 3 dots, then only a single value is selected
   from each row of data and a sequence of those values is returned.
   - If the pattern is a vector of multiple vars, then only the first row of data is processed
   and a single vector containing all of the requested results is returned. "
  [{:keys [new-node node-label] :as store-fns}
   [v :as pattern] :- [s/Any]
   data :- Results]
  (let [length (count pattern)
        columns (:cols (meta data))]
    (cond
      (and (= 2 length) (= '. (second pattern))) (project-single v columns data)
      (and (= 1 length) (vector? v)) (if (and (= 2 (count v)) (= '... (nth v 1)))
                                       (project-collection (first v) columns data)
                                       (project-tuple v columns data))
      :default (project-results store-fns pattern columns data))))

