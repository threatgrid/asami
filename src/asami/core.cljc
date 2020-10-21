(ns ^{:doc "A storage implementation over in-memory indexing. Includes full query engine."
      :author "Paula Gearon"}
    asami.core
    (:require [clojure.string :as str]
              [asami.storage :as storage :refer [ConnectionType DatabaseType]]
              [asami.memory :as memory]
              [asami.query :as query]
              [asami.internal :as internal]
              [asami.datom :as datom :refer [->Datom]]
              [asami.graph :as gr]
              [zuko.util :as util]
              [zuko.node :as node]
              [zuko.entity.general :as entity :refer [EntityMap GraphType]]
              [zuko.entity.writer :as writer :refer [Triple]]
              [zuko.entity.reader :as reader]
              #?(:clj  [schema.core :as s]
                 :cljs [schema.core :as s :include-macros true])))

(defonce connections (atom {}))

(s/defn ^:private parse-uri :- {:type s/Str
                                :name s/Str}
  "Splits up a database URI string into structured parts"
  [uri :- s/Str]
  (if (map? uri)
    uri
    (if-let [[_ db-type db-name] (re-find #"asami:([^:]+)://(.+)" uri)]
      {:type db-type
       :name db-name})))

(defn- connection-for
  "Creates a connection for a URI"
  [uri]
  (let [{:keys [type name]} (parse-uri uri)]
    (case type
      "mem" (memory/new-connection name memory/empty-graph) 
      "multi" (memory/new-connection name memory/empty-multi-graph) 
      "local" (throw (ex-info "Local Databases not yet implemented" {:type type :name name}))
      (throw (ex-info (str "Unknown graph URI schema" type) {:uri uri :type type :name name})))))

(s/defn create-database :- s/Bool
  "Creates database specified by uri. Returns true if the
   database was created, false if it already exists."
  [uri :- s/Str]
  (boolean
   (if-not (@connections uri)
     (swap! connections assoc uri (connection-for uri)))))

(s/defn connect :- ConnectionType
  "Connects to the specified database, returning a Connection.
  In-memory databases get created if they do not exist already.
  Memory graphs:
  asami:mem://dbname    A standard graph
  asami:multi://dbname  A multigraph"
  [uri :- s/Str]
  (if-let [conn (@connections uri)]
    conn
    (do
      (create-database uri)
      (@connections uri))))

(s/defn delete-database :- s/Bool
  "Deletes the database specified by the URI.
   Returns true if the delete occurred."
  [uri :- s/Str]
  ;; retrieve the database from connections
  (if-let [conn (@connections uri)]
    (do
      (swap! connections dissoc uri)
      (storage/delete-database conn))
    ;; database not in the connections
    ;; connect to it to free its resources
    (if-let [conn (connection-for uri)]
      (storage/delete-database conn))))

(def Graphable (s/cond-pre GraphType {:graph GraphType}))

(defn ^:private as-graph
  "Converts the d argument to a Graph. Leaves it alone if it can't be converted."
  [d]
  (if (not (satisfies? gr/Graph d))
    (let [g (:graph d)]
      (if (satisfies? gr/Graph g) g d))
      d))

(s/defn as-connection :- ConnectionType
  "Creates a Database/Connection around an existing Graph.
   graph: The graph or graph wrapper to build a database around.
   uri: The uri of the database."
  ([graph :- Graphable] (as-connection graph (gensym "internal:graph")))
  ([graph :- Graphable
    uri :- s/Str]
   (let [{:keys [name]} (parse-uri uri)
         c (memory/new-connection name (as-graph graph))]
     (swap! connections assoc uri c)
     c)))

(defn ^:private annotated-attribute?
  "Checks if an attribute has been annotated with a character"
  [c a]  ;; usually a keyword, but attributes can be other things
  (and (keyword a) (= c (last (name a)))))

(def ^:private update-attribute?
  "Checks if an attribute indicates that it should be updated"
  (partial annotated-attribute? \'))

(def ^:private append-attribute?
  "Checks if an attribute indicates that the data is an array that should be appended to"
  (partial annotated-attribute? \+))

(defn- normalize-attribute
  "Converts an updating attribute to its normalized form"
  [a]
  (if-not (keyword? a)
    a
    (let [n (name a)]
      (keyword (namespace a) (subs n 0 (dec (count n)))))))

(s/defn ^:private contains-updates?
  "Checks if any part of the object is to be updated"
  [obj :- {s/Any s/Any}]
  (let [obj-keys (keys obj)]
    (or (some update-attribute? obj-keys)
        (some append-attribute? obj-keys)
        (some #(and (map? %) (contains-updates? %)) (vals obj)))))

(s/defn ^:private entity-triples :- [(s/one [Triple] "New triples")
                                     (s/one [Triple] "Retractions")
                                     (s/one {s/Any s/Any} "New list of ID mappings")]
  "Creates the triples to be added and removed for a new entity.
   graph: the graph the entity is to be added to
   obj: The entity to generate triples for
   existing-ids: When IDs are provided by the user, then they get mapped to the internal ID that is actually used.
                 This map contains a mapping of user IDs to the ID allocated for the entity"
  [graph :- GraphType
   {id :db/id ident :db/ident :as obj} :- EntityMap
   existing-ids :- {s/Any s/Any}]
  (let [[new-obj removals additions]
        (if (contains-updates? obj)
          (do
            (when-not (or id ident)
              (throw (ex-info "Nodes to be updated must be identified with :db/id or :db/ident" obj)))
            (let [node-ref (if id
                             (and (seq (gr/resolve-triple graph id '?a '?v)) id)
                             (ffirst (gr/resolve-triple graph '?r :db/ident ident)))
                  _ (when-not node-ref (throw (ex-info "Cannot update a non-existent node" (select-keys obj [:db/id :db/ident]))))
                  ;; find the annotated attributes
                  obj-keys (keys obj)
                  update-attributes (set (filter update-attribute? obj-keys))
                  append-attributes (filter append-attribute? obj-keys)
                  ;; map annotated attributes to the unannotated form
                  attribute-map (->> (concat update-attributes append-attributes)
                                     (map (fn [a] [a (normalize-attribute a)]))
                                     (into {}))
                  ;; update attributes get converted, append attributes get removed
                  clean-obj (->> obj
                                 (keep (fn [[k v :as e]] (if-let [nk (attribute-map k)] (when (update-attributes k) [nk v]) e)))
                                 (into {}))
                  ;; find existing attribute/values that match the updates
                  entity-av-pairs (gr/resolve-triple graph node-ref '?a '?v)
                  update-attrs (set (map attribute-map update-attributes))
                  ;; determine what needs to be removed
                  removal-pairs (filter (comp update-attrs first) entity-av-pairs)
                  removals (mapcat (partial writer/existing-triples graph node-ref) removal-pairs)

                  ;; find the lists that the appending attributes refer to
                  append-attrs (set (map attribute-map append-attributes))
                  ;; find what should be the heads of lists, removing any that aren't list heads
                  attr-heads (->> entity-av-pairs
                                  (filter (comp append-attrs first))
                                  (filter #(seq (gr/resolve-triple graph (second %) :tg/first '?v))))
                  ;; find any appending attributes that are not in use. These are new arrays
                  remaining-attrs (reduce (fn [attrs [k v]] (disj attrs k)) append-attrs attr-heads)
                  ;; reassociate the object with any attributes that are for new arrays, making it a singleton array
                  append->annotate (into {} (map (fn [a] [(attribute-map a) a]) append-attributes))
                  new-obj (reduce (fn [o a] (assoc o a [(obj (append->annotate a))])) clean-obj remaining-attrs)
                  ;; find tails function
                  find-tail (fn [node]
                              (if-let [n (ffirst (gr/resolve-triple graph node :tg/rest '?r))]
                                (recur n)
                                node))
                  ;; create appending triples
                  append-triples (mapcat (fn [[attr head]]
                                           (let [v (obj (append->annotate attr))
                                                 new-node (node/new-node graph)]
                                             [[(find-tail head) :tg/rest new-node] [new-node :tg/first v] [head :tg/contains v]])) attr-heads)]
              [new-obj removals append-triples]))
          [obj nil nil])]
    (let [[triples ids] (writer/ident-map->triples graph new-obj existing-ids)
          ;; if updates occurred new new entity statements are redundant
          triples (if (or (seq removals) (seq additions) (not (identical? obj new-obj)))
                    (remove #(= :tg/entity (second %)) triples)
                    triples)]
      [(concat triples additions) removals ids])))

(defn- vec-rest
  "Takes a vector and returns a vector of all but the first element. Same as (vec (rest s))"
  [s]
  #?(:clj (subvec (vec s) 1)
     :cljs (vec (rest s))))

(defn- temp-id?
  "Tests if an entity ID is a temporary ID"
  [i]
  (and (number? i) (neg? i)))

(s/defn ^:private build-triples :- [(s/one [Triple] "Data to be asserted")
                                    (s/one [Triple] "Data to be retracted")
                                    (s/one {s/Any s/Any} "ID map of created objects")]
  "Converts a set of transaction data into triples.
  Returns a tuple containing [triples removal-triples tempids]"
  [{graph :graph :as db} :- DatabaseType
   data :- [s/Any]]
  (let [[retractions new-data] (util/divide' #(= :db/retract (first %)) data)
        add-triples (fn [[acc racc ids] obj]
                      (if (map? obj)
                        (let [[triples rtriples new-ids] (entity-triples graph obj ids)]
                          [(into acc triples) (into racc rtriples) new-ids])
                        (if (and (seqable? obj)
                                 (= 4 (count obj))
                                 (= :db/add (first obj)))
                          (or
                           (if (= (nth obj 2) :db/id)
                             (let [id (nth obj 3)]
                               (if (temp-id? id)
                                 (let [new-id (or (ids id) (node/new-node graph))]
                                   [(conj acc (assoc (vec-rest obj) 2 new-id)) racc (assoc ids (or id new-id) new-id)]))))
                           [(conj acc (mapv #(ids % %) (rest obj))) racc ids])
                          (throw (ex-info (str "Bad data in transaction: " obj) {:data obj})))))
        [triples rtriples id-map] (reduce add-triples [[] (vec retractions) {}] new-data)]
    [triples rtriples id-map]))

(def db storage/db)
(def as-of storage/as-of)
(def as-of-t storage/as-of-t)
(def since storage/since)
(def since-t storage/since-t)
(def graph storage/graph)
(def entity storage/entity)

(s/defn transact
  ;; returns a deref'able object that derefs to:
  ;; {:db-before DatabaseType
  ;;  :db-after DatabaseType
  ;;  :tx-data [datom/DatomType]
  ;;  :tempids {s/Any s/Any}}
  "Updates a database. This is currently synchronous, but returns a future or delay for compatibility with Datomic.
   connection: The connection to the database to be updated.
   tx-info: This is either a seq of items to be transacted, or a map containing a :tx-data value with such a seq.
            Each item to be transacted is one of:
            - vector of the form: [:db/add entity attribute value] - creates a datom
            - vector of the form: [:db/retract entity attribute value] - removes a datom
            - map: an entity to be inserted/updated.
  Entities and assertions may have attributes that are keywords with a trailing ' character.
  When these appear an existing attribute without that character will be replaced. This only occurs for the top level
  entity, and is not applied to attributes appearing in nested structures.
  Entities can be assigned a :db/id value. If this is a negative number, then it is considered a temporary value and
  will be mapped to a system-allocated ID. Other entities can reference such an entity using that ID.
  Entities can be provided a :db/ident value of any type. This will be considered unique, and can be used to identify
  entities for updates in subsequent transactions.

  Returns a future/delay object that will hold a map containing the following:
  :db-before    database value before the transaction
  :db-after     database value after the transaction
  :tx-data      sequence of datoms produced by the transaction
  :tempids      mapping of the temporary IDs in entities to the allocated nodes"
  [{:keys [name state] :as connection} :- ConnectionType
   {tx-data :tx-data :as tx-info} :- (s/if map? {:tx-data [s/Any]} [s/Any])]
  (let [op (fn []
             (let [tx-id (count (:history @state))
                   as-datom (fn [assert? [e a v]] (->Datom e a v tx-id assert?))
                   {:keys [graph history] :as db-before} (:db @state)
                   tx-data (or tx-data tx-info) ;; capture the old usage which didn't have an arg map
                   [triples removals tempids] (build-triples db-before tx-data)
                   [db-before db-after] (storage/transact-data connection triples removals)]
               {:db-before db-before
                :db-after db-after
                :tx-data (concat
                          (map (partial as-datom false) removals)
                          (map (partial as-datom true) triples))
                :tempids tempids}))]
    #?(:clj (future (op))
       :cljs (let [d (delay (op))]
               (force d)
               d))))

(defn- graphs-of
  "Converts Database objects to the graph that they wrap. Other arguments are returned unmodified."
  [inputs]
  (map (fn [d]
         (if (satisfies? storage/Database d)
           (storage/graph d)
           (as-graph d)))
       inputs))

(defn q
  "Execute a query against the provided inputs.
   The query can be a map, a seq, or a string.
   See the documentation at https://github.com/threatgrid/asami/wiki/Querying
   for a full description of queries.
   The end of the parameters may include a series of key/value pairs for query options.
   The only recognized option for now is:
     `:planner :user`
   This ensures that a query is executed in user-specified order, and not the order calculated by the optimizing planner."
  [query & inputs]
  (query/query-entry query memory/empty-graph (graphs-of inputs) false))

(defn show-plan
  "Return a query plan and do not execute the query.
   All parameters are identical to the `q` function."
  [query & inputs]
  (query/query-entry query memory/empty-graph (graphs-of inputs) true))
