(ns ^{:doc "Common internal elements of storage"
      :author "Paula Gearon"}
    asami.intern
  (:require [schema.core :as s :refer [=>]]
            [clojure.string :as string]
            #?(:cljs [cljs.core :refer [Symbol]])
            [asami.graph :as gr])
  #?(:clj (:import [clojure.lang Symbol])))


(def data-property :naga/first)

(def container-property :naga/contains)

(defn new-node
  "Create a new node"
  []
  (->> "node-"
       gensym
       name
       (keyword "mem")))

(defn node-id
  "Retrieve the unique ID of a node"
  [n]
  (subs (name n) 5))

(defn node-type?
  "Test if the presented object is an internal node"
  [value]
  (and (keyword? value)
       (= "mem" (namespace value))
       (string/starts-with? (name value) "node-")))

(defn node-label
  "Returns a keyword label for a node"
  [n]
  (keyword "asami" (str "id-" (node-id n))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Type definitions for projection

(def EntityPropertyElt
  (s/cond-pre s/Keyword s/Symbol #?(:clj Long :cljs s/Num)))

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

(def Value (s/pred (complement symbol?) "Value"))

(def Results [[Value]])

(def EntityPropAxiomElt
  (s/cond-pre s/Keyword #?(:clj Long :cljs s/Num)))

(def EntityPropValAxiomElt
  (s/conditional (complement symbol?) s/Any))

(def Axiom
  [(s/one EntityPropAxiomElt "entity")
   (s/one EntityPropAxiomElt "property")
   (s/one EntityPropValAxiomElt "value")])

(s/defn vartest? :- s/Bool
  [x]
  (and (symbol? x) (boolean (#{\? \%} (first (name x))))))

(s/defn vars :- [s/Symbol]
  "Return a seq of all variables in a pattern"
  [pattern :- EPVPattern]
  (filter vartest? pattern))

(s/defn project-row :- [s/Any]
  "Creates a new EPVPattern from an existing one, based on existing bindings.
   Uses the mapping to copy from columns in 'row' to overwrite variables in 'pattern'.
   'pattern' must be a vector.
   The index mappings have already been found and are in the 'mapping' argument"
  [wide-pattern :- [s/Any]
   nodes :- (s/maybe [s/Num])
   mapping :- {s/Num s/Num}
   row :- [Value]]
  (let [get-node (memoize (fn [n] (new-node)))
        node-statements (mapcat (fn [i]
                                  (let [node (get-node i)]
                                    [node :db/ident (node-label node)]))
                                nodes)
        update-pattern (fn [p [t f]]
                         (let [v (if (neg? f) (get-node f) (nth row f))]
                           (assoc p t v)))]
    (concat node-statements
            (reduce update-pattern wide-pattern mapping))))

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
         (keep-indexed
          (fn [n v] (if (and (nil? (var-positions n)) (vartest? v)) [n (fresh-map v)])))
         (into var-positions))))

(s/defn new-nodes :- [s/Num]
  "Returns all the new node references that appears in a map of offsets.
   Node references are negative numbers."
  [offset-map :- {s/Num s/Num}]
  (seq (set (filter neg? (vals offset-map)))))

(s/defn group-exists? :- [s/Any]
  "Determines if a group is instantiating a new piece of data,
   and if so checks if it already exists."
  [graph
   group :- [Axiom]]
  (if-let [[entity _ val :as g] (some (fn [[_ a _ :as axiom]] (when (= a :db/ident) axiom)) group)]
    (seq (gr/resolve-pattern graph ['?e :db/ident val]))))

(s/defn adorn-entities :- [Axiom]
  "Marks new entities as stored entities"
  [triples :- [Axiom]]
  (reduce (fn [acc [e a v :as triple]]
            (let [r (conj acc triple)]
              (if (= :db/ident a) (conj r [e :naga/entity true]) r)))
          []
          triples))

(s/defn project :- Results
  "Converts each row from a result, into just the requested columns, as per the patterns arg.
   Any specified value in the patterns will be copied into that position in the projection.
   Unbound patterns will generate new nodes for each row.
  e.g. For pattern [?h1 :friend ?h2]
       data: [[h1=frodo h3=bilbo h2=gandalf]
              [h1=merry h3=pippin h2=frodo]]
  leads to: [[h1=frodo :friend h2=gandalf]
             [h1=merry :friend h2=frodo]]"
  [pattern :- [s/Any]
   data :- Results]
  (let [full-pattern (vec pattern)
        columns (:cols (meta data))
        pattern->data (offset-mappings full-pattern columns data)
        nodes (new-nodes pattern->data)]
    (with-meta
      (map #(project-row full-pattern nodes pattern->data %) data)
      {:cols full-pattern})))

(s/defn insert-project :- Results
  "Similar to project, only the generated data will be in triples for insertion.
   If triples describe entities with existing dc/ident fields, then they will be dropped."
  [graph
   patterns :- [[s/Any]]
   #_vars-to-update ;; :- #{Var}  ;; These vars are being updated during projection. Bind them as extra droppable cols
   columns :- [Symbol]
   data :- Results]
  (let [full-pattern (vec (apply concat patterns))
        pattern->data (offset-mappings full-pattern columns data)
        nodes (new-nodes pattern->data)]
    (->> data
         (map #(partition 3 (project-row full-pattern nodes pattern->data %)))
         (remove (partial group-exists? graph))
         (apply concat)
         adorn-entities)))
