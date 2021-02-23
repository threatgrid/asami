(ns ^{:doc "A storage implementation over in-memory indexing. Includes full query engine."
      :author "Paula Gearon"}
    asami.core
    (:require [asami.storage :as storage :refer [ConnectionType DatabaseType]]
              [asami.memory :as memory]
              [asami.query :as query]
              [asami.datom :as datom :refer [->Datom]]
              [asami.graph :as gr]
              [asami.entities :as entities]
              [zuko.entity.general :refer [GraphType]]
              #?(:clj  [clojure.edn :as edn]
                 :cljs [cljs.reader :as edn])
              #?(:clj  [schema.core :as s]
                 :cljs [schema.core :as s :include-macros true]))
    #?(:clj (:import (java.util.concurrent CompletableFuture)
                     (java.util.function Supplier))))

(defonce connections (atom {}))

(s/defn ^:private parse-uri :- {:type s/Str
                                :name s/Str}
  "Splits up a database URI string into structured parts"
  [uri]
  (if (map? uri)
    uri
    (if-let [[_ db-type db-name] (re-find #"asami:([^:]+)://(.+)" uri)]
      {:type db-type
       :name db-name}
      (throw (ex-info (str "Invalid URI: " uri) {:uri uri})))))

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
  ([graph :- Graphable] (as-connection graph (str (gensym "asami:mem://internal"))))
  ([graph :- Graphable
    uri :- s/Str]
   (let [{:keys [name]} (parse-uri uri)
         c (memory/new-connection name (as-graph graph))]
     (swap! connections assoc uri c)
     c)))

(def db storage/db)
(def as-of storage/as-of)
(def as-of-t storage/as-of-t)
(def since storage/since)
(def since-t storage/since-t)
(def graph storage/graph)
(def entity storage/entity)

(def TransactData (s/if map?
                    {(s/optional-key :tx-data) [s/Any]
                     (s/optional-key :tx-triples) [[(s/one s/Any "entity")
                                                    (s/one s/Any "attribute")
                                                    (s/one s/Any "value")]]
                     (s/optional-key :executor) s/Any
                     (s/optional-key :update-fn) (s/pred fn?)}
                    [s/Any]))

(s/defn transact
  ;; returns a deref'able object that derefs to:
  ;; {:db-before DatabaseType
  ;;  :db-after DatabaseType
  ;;  :tx-data [datom/DatomType]
  ;;  :tempids {s/Any s/Any}}
  "Updates a database. This is currently synchronous, but returns a future or delay for compatibility with Datomic.
   connection: The connection to the database to be updated.
   tx-info: This is either a seq of items to be transacted, or a map.
            If this is a map, then a :tx-data value will contain the same type of seq that tx-info may have.
            Each item to be transacted is one of:
            - vector of the form: [:db/add entity attribute value] - creates a datom
            - vector of the form: [:db/retract entity attribute value] - removes a datom
            - map: an entity to be inserted/updated.
            Alternatively, a map may have a :tx-triples key. If so, then this is a seq of 3 element vectors.
            Each vector in a :tx-triples seq will contain the raw values for [entity attribute value]
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
  :tempids      mapping of the temporary IDs in entities to the allocated nodes
  :executor     Executor to be used to run the CompletableFuture"
  [{:keys [name state] :as connection} :- ConnectionType
   {:keys [tx-data tx-triples executor update-fn] :as tx-info} :- TransactData]
  (let [op (if update-fn
             (fn []
               (let [[db-before db-after] (storage/transact-update connection update-fn)]
                 {:db-before db-before
                  :db-after db-after}))
             (fn []
               (let [tx-id (count (:history @state))
                     as-datom (fn [assert? [e a v]] (->Datom e a v tx-id assert?))
                     {:keys [graph history] :as db-before} (:db @state)
                     [triples removals tempids] (if tx-triples ;; is this inserting raw triples?
                                                  [tx-triples nil {}]
                                                  ;; capture the old usage which didn't have an arg map
                                                  (entities/build-triples db-before (or tx-data tx-info)))
                     [db-before db-after] (storage/transact-data connection triples removals)]
                 {:db-before db-before
                  :db-after db-after
                  :tx-data (concat
                            (map (partial as-datom false) removals)
                            (map (partial as-datom true) triples))
                  :tempids tempids})))]
    #?(:clj (CompletableFuture/supplyAsync (reify Supplier (get [_] (op)))
                                           (or executor clojure.lang.Agent/soloExecutor))
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

(defn export-data
  "Returns a simplified data structures of all statements in a database"
  [database]
  (let [g (if (satisfies? storage/Database database)
            (storage/graph database)
            (as-graph database))]
    (gr/resolve-pattern g '[?e ?a ?v ?t]))) ;; Note that transactions are not returned yet

(defn import-data
  "Loads raw statements into a connection. This does no checking of existing contents of storage.
  Accepts either a seq of tuples, or an EDN string which contains a seq of tuples"
  [connection data]
  (let [statements (if (string? data)
                     (edn/read-string data)
                     data)]
    (transact connection {:tx-triples statements})))
