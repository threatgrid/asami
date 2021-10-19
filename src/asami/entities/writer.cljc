(ns ^{:doc "Converts external data into a graph format (triples)."
      :author "Paula Gearon"}
    asami.entities.writer
  (:require [asami.entities.general :as general :refer [tg-ns KeyValue EntityMap GraphType]]
            [asami.entities.reader :as reader]
            [zuko.node :as node]
            [schema.core :as s :refer [=>]]
            [clojure.string :as string]))

;; internal generated properties:
;; :tg/rest List structure
;; :tg/owns References sub entities
;; :tg/entity When true, then indicates a top level entity

;; The following 2 attributes may vary according to the database.
;; e.g. Datomic appends -s -l -d etc to these attributes for different datatypes
;; Asami uses these names without modification:
;; :tg/first Indicates a list member by position. Returned by node/data-attribute
;; :tg/contains Shortcut to list members. Returned by node/container-attribute

;; The following are graph nodes with special meaning:
;; :tg/emtpty-list A list without entries
;; :tg/nil a nil value


;; provides dynamic scope of the current contents of the graph
;; This approach has been adopted to avoid redundantly passing the graph down the callstack
(def ^:dynamic *current-graph* nil)

;; The following provide dynamic scope of accumulated state through the
;; conversion of entities into triples. This approach has been adopted for speed.
(def ^:dynamic *id-map* nil)

(def ^:dynamic *triples* nil)

(def ^:dynamic *limit* nil)

(def ^:dynamic *current-entity* nil)

(def ^:dynamic *top-level-entities* nil)

(def Triple [(s/one s/Any "Entity")
             (s/one s/Keyword "attribute")
             (s/one s/Any "value")])

(declare value-triples map->triples)

(defn add-triples!
  [op data]
  (vswap! *triples* op data)
  (when (and *limit*
             (> (count @*triples*) *limit*))
    (throw (ex-info "overflow" {:overflow true}))))

(defn list-triples
  "Creates the triples for a list. Returns a node and list of nodes representing contents of the list."
  [vlist]
  (when (seq vlist)
    (loop [list-ref nil, last-ref nil, value-nodes [], [v & vs :as val-list] vlist]
      (if-not (seq val-list)
        [list-ref value-nodes]
        (let [node-ref (node/new-node *current-graph*)
              _ (when last-ref
                  (add-triples! conj [last-ref :tg/rest node-ref]))
              value-ref (value-triples v)]
          (add-triples! conj [node-ref (node/data-attribute *current-graph* value-ref) value-ref])
          (recur (or list-ref node-ref) node-ref (conj value-nodes value-ref) vs))))))

(s/defn value-triples-list
  [vlist :- [s/Any]]
  (if (seq vlist)
    (let [[node value-nodes] (list-triples vlist)]
      (doseq [vn value-nodes]
        (add-triples! conj [node (node/container-attribute *current-graph* vn) vn]))
      node)
    :tg/empty-list))

(defn lookup-ref?
  "Tests if i is a lookup ref"
  [i]
  (and (vector? i) (keyword? (first i)) (= 2 (count i))))

(defn resolve-ref
  [[prop id]]
  (or (and (= :db/id prop) (get @*id-map* id id))
      (ffirst (node/find-triple *current-graph* ['?n prop id]))))

(defn top-level-entity?
  [node]
  (seq (node/find-triple *current-graph* [node :tg/entity true])))

(defn add-subentity-relationship
  "Adds a sub-entity relationship for a provided node. Returns the node"
  [node]
  (when-not (or (= node *current-entity*)
                (@*top-level-entities* node)
                (= node :tg/empty-list))
    (add-triples! conj [*current-entity* :tg/owns node]))
  node)

(defn value-triples
  "Converts a value into a list of triples.
   Return the entity ID of the data."
  [v]
  (cond
    (lookup-ref? v) (or (resolve-ref v)
                        (value-triples-list v))
    (sequential? v) (-> (value-triples-list v) add-subentity-relationship)
    (set? v) (value-triples-list (seq v))
    (map? v) (-> (map->triples v) add-subentity-relationship)
    (nil? v) :tg/nil
    :default v))

(s/defn property-vals
  "Takes a property-value pair associated with an entity,
   and builds triples around it"
  [entity-ref :- s/Any
   [property value] :- KeyValue]
  (if (set? value)
    (doseq [v value]
      (let [vr (value-triples v)]
        (add-triples! conj [entity-ref property vr])))
    (let [v (value-triples value)]
      (add-triples! conj [entity-ref property v]))))

(defn new-node
  [id]
  (let [next-id (node/new-node *current-graph*)]
    (when id
      (vswap! *id-map* assoc (or id next-id) next-id))
    next-id))

(s/defn get-ref
  "Returns the reference (a node-id) for an object, and a flag that is false if a new reference was generated"
  [{id :db/id ident :db/ident ident2 :id :as data} :- {s/Keyword s/Any}]
  (if-let [r (@*id-map* id)] ;; an ID that is already mapped
    [r false]
    (let [idd (or ident ident2)]
      (cond ;; a negative ID is a request for a new saved ID
        (and (number? id) (neg? id)) (let [new-id (new-node id)]
                                       (when idd
                                         (vswap! *id-map* assoc idd new-id))
                                       [new-id false])
        ;; Use the provided ID
        id (if (node/node-type? *current-graph* :db/id id)
             [id false]
             (throw (ex-info ":db/id must be a value node type" {:db/id id})))
        ;; With no ID do an ident lookup
        ident (if-let [r (@*id-map* idd)]
                [r true]
                (let [lookup (if ident
                               (node/find-triple *current-graph* ['?n :db/ident ident])
                               (node/find-triple *current-graph* ['?n :id ident2]))]
                  (if (seq lookup)
                    (let [read-id (ffirst lookup)]
                      (when (top-level-entity? read-id)
                        (vswap! *top-level-entities* conj read-id))
                      (vswap! *id-map* assoc idd read-id)
                      [read-id true]) ;; return the retrieved ref
                    [(new-node idd) false]))) ;; nothing retrieved so generate a new ref
        ;; generate an ID
        :default [(new-node nil) false]))))  ;; generate a new ref


(s/defn map->triples
  "Converts a single map to triples. Returns the entity reference or node id.
   The triples are built up statefully in the volatile *triples*."
  [data :- {s/Keyword s/Any}]
  (let [[entity-ref ident?] (get-ref data)
        data (dissoc data :db/id)
        data (if ident? (dissoc data :db/ident) data)]
    ;; build up result in *triples*
    ;; duplicating the code on both branches of the condition,
    ;; in order to avoid an unnecessary binding on the stack
    (if *current-entity*
      (doseq [d data]
        (property-vals entity-ref d))
      (binding [*current-entity* entity-ref]
        (vswap! *top-level-entities* conj entity-ref)
        (doseq [d data]
          (property-vals entity-ref d))))
    entity-ref))


(defn name-for
  "Convert an id (probably a number) to a keyword for identification"
  [id]
  (if (or (keyword? id) (node/node-type? *current-graph* :db/id id))
    id
    (node/node-label *current-graph* id)))


(s/defn ident-map->triples
  "Converts a single map to triples for an ID'ed map"
  ([graph :- GraphType
    j :- EntityMap]
   (ident-map->triples graph j {} #{} nil))
  ([graph :- GraphType
    j :- EntityMap
    id-map :- {s/Any s/Any}
    top-level-ids :- #{s/Any}
    limit :- (s/maybe s/Num)]
   (binding [*current-graph* graph
             *id-map* (volatile! id-map)
             *triples* (volatile! [])
             *limit* limit
             *top-level-entities* (volatile! top-level-ids)]
     (let [derefed-id-map (ident-map->triples j)]
       [@*triples* derefed-id-map @*top-level-entities*])))
  ([j :- EntityMap]
   (let [node-ref (map->triples j)]
     (if (:db/ident j)
       (add-triples! conj [node-ref :tg/entity true])
       (add-triples! into [[node-ref :db/ident (name-for node-ref)] [node-ref :tg/entity true]]))
     @*id-map*)))

(defn backtrack-unlink-top-entities
  "Goes back through generated triples and removes sub-entity links to entities that were later
  determined to be top-level entities."
  [top-entities triples]
  (remove #(and (= :tg/owns (nth % 1)) (top-entities (nth % 2))) triples))

(s/defn entities->triples :- [Triple]
  "Converts objects into a sequence of triples."
  ([graph :- GraphType
    entities :- [EntityMap]]
   (entities->triples graph entities {}))
  ([graph :- GraphType
    entities :- [EntityMap]
    id-map :- {s/Any s/Any}]
   (binding [*current-graph* graph
             *id-map* (volatile! id-map)
             *triples* (volatile! [])
             *top-level-entities* (volatile! #{})]
     (doseq [e entities]
       (ident-map->triples e))
     ;; backtrack to see if there were forward references to top level entities
     (backtrack-unlink-top-entities @*top-level-entities* @*triples*))))


;; updating the store

(s/defn existing-triples
  [graph :- GraphType
   node-ref
   [k v]]
  (or
   (if-let [subpv (reader/check-structure graph k v)]
     (if-not (some #(= :tg/entity (first %)) subpv) 
       (cons [node-ref k v] (mapcat (partial existing-triples graph v) subpv))))
   [[node-ref k v]]))

(s/defn entity-update->triples :- [(s/one [Triple] "assertions") (s/one [Triple] "retractions")]
  "Takes a single structure and converts it into triples to be added and triples to be retracted to create a change"
  [graph :- GraphType
   node-ref  ;; a reference for the structure to be updated
   entity]   ;; the structure to update the structure in the database to
  (binding [*current-graph* graph
            *id-map* (volatile! {})]
    (let [pvs (reader/property-values graph node-ref)
          old-struct (reader/pairs->struct graph pvs)
          to-remove (remove (fn [[k v]] (if-let [newv (get entity k)] (= v newv))) old-struct)
          pvs-to-remove (filter (comp (set (map first to-remove)) first) pvs)
          triples-to-remove (mapcat (partial existing-triples graph node-ref) pvs-to-remove)

          to-add (remove (fn [[k v]] (when-let [new-val (get old-struct k)] (= new-val v))) entity)
          triples-to-add (binding [*triples* (volatile! [])
                                   *top-level-entities* (volatile! #{})
                                   *current-entity* node-ref]
                           (doseq [pvs to-add] (property-vals node-ref pvs))
                           (backtrack-unlink-top-entities @*top-level-entities* @*triples*))]
      [triples-to-add triples-to-remove])))
