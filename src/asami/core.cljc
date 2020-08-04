(ns ^{:doc "A storage implementation over in-memory indexing. Includes full query engine."
      :author "Paula Gearon"}
    asami.core
    (:require [clojure.string :as str]
              [asami.graph :as gr]
              [asami.index :as mem]
              [asami.multi-graph :as multi]
              [asami.query :as query]
              [asami.internal :as internal]
              [asami.datom :as datom :refer [->Datom]]
              [naga.store :as store :refer [Storage StorageType]]
              [zuko.util :as util]
              [zuko.entity.general :as entity :refer [EntityMap GraphType]]
              [zuko.entity.writer :as writer :refer [Triple]]
              [zuko.entity.reader :as reader]
              #?(:clj  [schema.core :as s]
                 :cljs [schema.core :as s :include-macros true]))
    #?(:clj (:import [java.util Date])))

(defn now
  "Creates an object to represent the current time"
  []
  #?(:clj (Date.)
     :cljs (js/Date.)))

(defn instant?
  "Tests if a value is a timestamp"
  [t]
  (= #?(:clj Date :cljs js/Date) (type t)))

(defn ^:private find-index
  "Performs a binary search through a sorted vector, returning the index of a provided value
   that is in the vector, or the next lower index if the value is not present.
   a: The vector to be searched
   v: The value being searched for
   cmp: A 2 argument comparator function (Optional).
        Defaults to clojure.core/compare
        Must return -1 when the first arg < the second arg.
                    +1 when the first arg > the second arg.
                    0 when the args are equal."
  ([a v]
   (find-index a v compare))
  ([a v cmp]
     (loop [low 0 high (count a)]
       (if (= (inc low) high)
         low
         (let [mid (int (/ (+ low high) 2))
               mv (nth a mid)
               c (cmp mv v)]
           (cond
             (zero? c) mid
             (> 0 c) (recur mid high)
             (< 0 c) (recur low mid)))))))

(defonce databases (atom {}))

(def empty-graph mem/empty-graph)
(def empty-multi-graph multi/empty-multi-graph)

;; graph is the wrapped graph
;; history is a seq of Databases, excluding this one
;; timestamp is the time the database was created
(defrecord Database [graph history timestamp])

(def DatabaseType (s/pred (partial instance? Database)))

;; name is the name of the database
;; state is an atom containing:
;; :db is the latest DB
;; :history is a list of tuples of Database, including db
(defrecord Connection [name state])

(def ConnectionType (s/pred (partial instance? Connection)))

(defn- new-empty-connection
  [name empty-gr]
  (let [db (->Database empty-gr [] (now))]
    (->Connection name (atom {:db db :history [db]}))))

(s/defn ^:private parse-uri :- {:type s/Str
                                :name s/Str}
  "Splits up a database URI string into structured parts"
  [uri :- s/Str]
  (if (map? uri)
    uri
    (if-let [[_ db-type db-name] (re-find #"asami:([^:]+)://(.+)" uri)]
      {:type db-type
       :name db-name})))

(s/defn create-database :- s/Bool
  "Creates database specified by uri. Returns true if the
   database was created, false if it already exists."
  [uri :- s/Str]
  (boolean
   (if-not (@databases uri)
     (let [{:keys [type name]} (parse-uri uri)]
       (case type
         "mem" (swap! databases assoc uri (new-empty-connection name empty-graph)) 
         "multi" (swap! databases assoc uri (new-empty-connection name empty-multi-graph)) 
         "local" (throw (ex-info "Local Databases not yet implemented" {:type type :name name}))
         (throw (ex-info (str "Unknown graph URI schema" type) {:uri uri :type type :name name})))))))

(s/defn connect :- ConnectionType
  "Connects to the specified database, returning a Connection.
  In-memory databases get created if they do not exist already.
  Memory graphs:
  asami:mem://dbname    A standard graph
  asami:multi://dbname  A multigraph"
  [uri :- s/Str]
  (if-let [conn (@databases uri)]
    conn
    (do
      (create-database uri)
      (@databases uri))))

(s/defn db :- DatabaseType
  "Retrieves the most recent value of the database for reading."
  [connection :- ConnectionType]
  (:db @(:state connection)))

(s/defn as-of :- DatabaseType
  "Retrieves the database as of a given moment, inclusive.
   The t value may be an instant, or a transaction ID.
   The database returned will be whatever database was the latest at the specified time or transaction."
  [{:keys [graph history timestamp] :as db} :- DatabaseType
   t :- (s/cond-pre s/Int (s/pred instant?))]
  (cond
    (instant? t) (if (>= (compare t timestamp) 0)
                   db
                   (nth history
                        (find-index history t #(compare (:timestamp %1) %2))))
    (int? t) (if (>= t (count history))
               db
               (nth history (min (max t 0) (dec (count history)))))))

(s/defn as-of-t :- s/Int
  "Returns the as-of point for a database, or nil if none"
  [{history :history :as db} :- DatabaseType]
  (and history (count history)))

(s/defn since :- (s/maybe DatabaseType)
  "Returns the value of the database since some point t, exclusive.
   t can be a transaction number, or instant."
  [{:keys [graph history timestamp] :as db} :- DatabaseType
   t :- (s/cond-pre s/Int (s/pred instant?))]
  (cond
    (instant? t) (cond
                   (> (compare t timestamp) 0) nil
                   (< (compare t (:timestamp (first history))) 0) (first history)
                   :default (let [tx (inc (find-index history t #(compare (:timestamp %1) %2)))]
                              (if (= tx (count history))
                                db
                                (nth history tx))))
    (int? t) (cond (>= t (count history)) nil
                   (= (count history) (inc t)) db
                   :default (nth history (min (max (inc t) 0) (dec (count history)))))))

(s/defn since-t :- s/Int
  "Returns the since point of a database, or nil if none"
  [{history :history :as db} :- DatabaseType]
  (if-not (seq history)
    0
    (count history)))

(s/defn graph :- GraphType
  "Returns the Graph object associated with a Database"
  [database :- DatabaseType]
  (:graph database))

(s/defn as-database :- DatabaseType
  "Creates a Database around an existing Graph.
   graph: The graph to build a database around.
   uri: Optional. The uri of the database. If this is provided then a connection will be available."
  ([graph :- GraphType]
   (->Database graph [] (now)))
  ([graph :- GraphType
    uri :- s/Str]
   (let [{:keys [name]} (parse-uri uri)
         new-conn (new-empty-connection name graph)]
     (swap! databases assoc uri new-conn)
     (db new-conn))))

(s/defn ^:private assert-data :- GraphType
  "Adds triples to a graph"
  [graph :- GraphType
   data :- [[s/Any]]]
  (query/add-to-graph graph data))

(s/defn ^:private retract-data :- GraphType
  "Removes triples from a graph"
  [graph :- GraphType
   data :- [[s/Any]]]
  (query/delete-from-graph graph data))

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
                                                 new-node (gr/new-node)]
                                             [[(find-tail head) :tg/rest new-node] [new-node :tg/first v] [head :tg/contains v]])) attr-heads)]
              [new-obj removals append-triples]))
          [obj nil nil])]
    (let [[triples ids] (writer/ident-map->triples graph new-obj existing-ids)]
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
                                 (let [new-id (or (ids id) (gr/new-node))]
                                   [(conj acc (assoc (vec-rest obj) 2 new-id)) racc (assoc ids (or id new-id) new-id)]))))
                           [(conj acc (mapv #(ids % %) (rest obj))) racc ids])
                          (throw (ex-info (str "Bad data in transaction: " obj) {:data obj})))))
        [triples rtriples id-map] (reduce add-triples [[] (vec retractions) {}] new-data)]
    [triples rtriples id-map]))

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
            - vector of the form: [:db/assert entity attribute value] - creates a datom
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
                   next-graph (-> graph
                                  (retract-data removals)
                                  (assert-data triples))
                   db-after (->Database
                             next-graph
                             (conj history db-before)
                             (now))]
               (reset! (:state connection) {:db db-after :history (conj (:history db-after) db-after)})
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

(s/defn entity :- {s/Any s/Any}
  "Returns an entity based on an identifier, either the :db/id or a :db/ident if this is available. This eagerly retrieves the entity.
   Objects may be nested, but references to top level objects will be nil in order to avoid loops."
  ;; TODO create an Entity type that lazily loads, and references the database it came from
  [{graph :graph :as db} id]
  (if-let [ref (or (and (seq (gr/resolve-triple graph id '?a '?v)) id)
                   (ffirst (gr/resolve-triple graph '?e :db/ident id)))]
    (reader/ref->entity graph ref)))

(defn- graphs-of
  "Converts Database objects to the graph that they wrap. Other arguments are returned unmodified."
  [inputs]
  (map #(if (instance? Database %) (:graph %) %) inputs))

(defn q
  "Execute a query against the provided inputs.
   The query can be a map, a seq, or a string.
   See the documentation at https://github.com/threatgrid/asami/wiki/Querying
   for a full description of queries."
  [query & inputs]
  (query/query-entry query mem/empty-graph (graphs-of inputs)))
