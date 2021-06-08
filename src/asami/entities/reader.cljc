(ns ^{:doc "Reads structured data from a graph."
      :author "Paula Gearon"}
    asami.entities.reader
  (:require [asami.entities.general :as general :refer [tg-ns KeyValue EntityMap GraphType]]
            [zuko.node :as node]
            [schema.core :as s :refer [=>]]
            [clojure.string :as string]))


(def MapOrList (s/cond-pre EntityMap [s/Any]))

(def NodeType s/Any) ;; No checking, but indicates a node in a graph

(defn get-tg-first
  "Finds the tg/first property in a map, and gets the value."
  [struct]
  (let [first-pair? (fn [[k v :as p]]
                      (and (keyword? k)
                           (= tg-ns (namespace k))
                           (string/starts-with? (name k) "first")
                           p))]
    (some first-pair? struct)))

(s/defn property-values :- [KeyValue]
  "Return all the property/value pairs for a given entity in the store. "
  [graph :- GraphType
   entity :- s/Any]
  (->> (node/find-triple graph [entity '?p '?o])
       (remove #(= :tg/owns (first %)))))


(s/defn check-structure :- (s/maybe [KeyValue])
  "Determines if a value represents a structure. If so, return the property/values for it.
   Otherwise, return nil."
  [graph :- GraphType
   prop :- s/Any
   v :- s/Any]
  (if (and (not (#{:db/ident :db/id} prop)) (node/node-type? graph prop v))
    (let [data (property-values graph v)]
      data)))


(declare pairs->struct recurse-node)

(s/defn build-list :- [s/Any]
  "Takes property/value pairs and if they represent a list node, returns the list.
   else, nil."
  [graph :- GraphType
   seen :- #{NodeType}
   pairs :- [KeyValue]]
  ;; convert the data to a map
  (let [st (into {} pairs)]
    ;; if the properties indicate a list, then process it
    (if-let [first-prop-elt (get-tg-first st)]
      (let [remaining (:tg/rest st)
            [_ first-elt] (recurse-node graph seen first-prop-elt)]
        (assert first-elt)
        (let [head-elt (if (= :tg/nil first-elt) nil first-elt)]
          ;; recursively build the list
          (if remaining
            (cons head-elt (build-list graph seen (property-values graph remaining)))
            (list head-elt))))
      (when (= :tg/list (:tg/type st)) []))))

(s/defn vbuild-list :- [s/Any]
  "Calls build-list, converting to a vector as the final step"
  [graph :- GraphType
   seen :- #{NodeType}
   pairs :- [KeyValue]]
  (let [l (build-list graph seen pairs)]
    (if (seq? l) (vec l) l)))

(def ^:dynamic *nested-structs* false)

(s/defn recurse-node :- s/Any
  "Determines if the val of a map entry is a node to be recursed on, and loads if necessary.
  If referring directly to a top level node, then short circuit and return the ID"
  [graph :- GraphType
   seen :- #{NodeType}
   [prop v :as prop-val] :- KeyValue]
  (if-let [pairs (check-structure graph prop v)]
    (if (and (not *nested-structs*) (some #(= :tg/entity (first %)) pairs))
      [prop (if-let [ident (some (fn [[k v]] (if (= :db/ident k) v)) pairs)]
              {:db/ident ident}
              {:db/id v})]
      [prop (or (vbuild-list graph seen pairs)
                (pairs->struct graph pairs (conj seen v)))])
    (if (= :tg/empty-list v)
      [prop []]
      prop-val)))


(s/defn into-multimap
  "Takes key/value tuples and inserts them into a map. If there are duplicate keys then create a set for the values."
  [kvs :- [[(s/one s/Any "Key") (s/one s/Any "Value")]]]
  #?(:clj 
     (persistent!
      (reduce (fn [m [k v]]
                (assoc! m k (if-let [[km vm] (find m k)]
                              (if (set? vm) (conj vm v) (hash-set vm v))
                              v)))
              (transient {}) kvs))
     :cljs
     (persistent!
      (reduce (fn [m [k v]]
                (assoc! m k (let [vm (get m k ::null)]
                              (if-not (= ::null vm)
                                (if (set? vm) (conj vm v) (hash-set vm v))
                                v))))
              (transient {}) kvs))))

(s/defn pairs->struct :- EntityMap
  "Uses a set of property-value pairs to load up a nested data structure from the graph"
  ([graph :- GraphType
    prop-vals :- [KeyValue]] (pairs->struct graph prop-vals #{}))
  ([graph :- GraphType
    prop-vals :- [KeyValue]
    seen :- #{NodeType}]
   (if (some (fn [[k _]] (= :tg/first k)) prop-vals)
     (vbuild-list graph seen prop-vals)
     (do
       (->> prop-vals
            (remove (comp #{:db/id :db/ident :tg/entity} first))  ;; INTERNAL PROPERTIES
            (remove (comp seen second))
            (map (fn [[a v :as av]] (if (= :tg/nil v) [a nil] av)))
            (map (partial recurse-node graph seen))
            (map (fn [[a v :as av]] (if (seq? v) [a (vec v)] av)))
            into-multimap)))))


(s/defn ref->entity :- EntityMap
  "Uses an id node to load up a nested data structure from the graph.
   Accepts a value that identifies the internal node."
  ([graph :- GraphType
    entity-id :- s/Any]
   (ref->entity graph entity-id false nil))
  ([graph :- GraphType
    entity-id :- s/Any
    nested? :- s/Bool]
   (ref->entity graph entity-id nested? nil))
  ([graph :- GraphType
    entity-id :- s/Any
    nested? :- s/Bool
    exclusions :- (s/maybe #{(s/cond-pre s/Keyword s/Str)})]
   (binding [*nested-structs* nested?]
     (let [prop-vals (property-values graph entity-id)
           pvs (if (seq exclusions)
                 (remove (comp exclusions first) prop-vals)
                 prop-vals)]
       (pairs->struct graph pvs)))))


(s/defn ident->entity :- EntityMap
  "Converts data in a database to a data structure suitable for JSON encoding
   Accepts an internal node identifier to identify the entity object"
  ([graph :- GraphType
    ident :- s/Any]
   (ident->entity graph ident false))
  ([graph :- GraphType
    ident :- s/Any
    nested? :- s/Bool]
   ;; find the entity by its ident. Some systems will make the id the entity id,
   ;; and the ident will be separate, so look for both.
   (let [eid (or (ffirst (node/find-triple graph [ident '?a '?v]))
                 (ffirst (node/find-triple graph ['?eid :db/ident ident])))]
     (ref->entity graph eid nested?))))

(s/defn graph->entities :- [EntityMap]
  "Pulls all top level entities out of a store"
  ([graph :- GraphType]
   (graph->entities graph false nil))
  ([graph :- GraphType
    nested? :- s/Bool]
   (graph->entities graph nested? nil))
  ([graph :- GraphType
    nested? :- s/Bool
    exclusions :- (s/maybe #{s/Keyword})]
   (->> (node/find-triple graph '[?e :tg/entity true])
        (map first)
        (map #(ref->entity graph % nested? exclusions)))))
