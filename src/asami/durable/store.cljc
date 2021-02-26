(ns ^{:doc "The implements the Block storage version of a Graph/Database/Connection"
      :author "Paula Gearon"}
    asami.durable.store
  (:require [asami.storage :as storage :refer [ConnectionType DatabaseType]]
            [asami.graph :as graph]
            [asami.internal :refer [now instant? long-time]]
            [asami.durable.common :as common :refer [append-tx! commit! get-tx latest tx-count find-tx close delete!]]
            [asami.durable.pool :as pool]
            [asami.durable.tuples :as tuples]
            [asami.durable.graph :as dgraph]
            [zuko.schema :refer [Triple]]
            [zuko.entity.general :refer [GraphType]]
            [zuko.entity.reader :as reader]
            [schema.core :as s :include-macros true]
            #?(:clj [asami.durable.flat-file :as flat-file])))

(def tx-name "tx.dat")

;; transactions contain tree roots for the 3 tree indices,
;; the tree root for the data pool
;; and a transaction timestamp
(def tx-record-size (* 5 common/long-size))

(def TxRecord {(s/required-key :r-spot) s/Int
               (s/required-key :r-post) s/Int
               (s/required-key :r-ospt) s/Int
               (s/required-key :r-pool) s/Int
               (s/required-key :timestamp) s/Int})

(def TxRecordPacked {(s/required-key :timestamp) s/Int
                     (s/required-key :tx-data) [s/Int]})

(s/defn pack-tx :- TxRecordPacked
  "Packs a transaction into a vector for serialization"
  [{:keys [r-spot r-post r-ospt r-pool timestamp]} :- TxRecord]
  {:timestamp timestamp :tx-data [r-spot r-post r-ospt r-pool]})

(s/defn unpack-tx :- TxRecord
  "Unpacks a transaction vector into a structure when deserializing"
  [{[r-spot r-post r-ospt r-pool] :tx-data timestamp :timestamp} :- TxRecordPacked]
  {:r-spot r-spot :r-post r-post :r-ospt r-ospt :r-pool r-pool :timestamp timestamp})

(declare ->DurableDatabase)

(s/defn as-of* :- DatabaseType
  "Returns a database value for a provided t-value.
  If t-val is the transaction number for an older database, then returns that database. Otherwise, will return this database
  If t-val is a timestamp then returns the most recent database that was valid at that time."
  [{{:keys [tx-manager] :as connection} :connection
    bgraph :bgraph
    timestamp :timestamp
    t :t :as database} :- DatabaseType
   t-val]
  (if-let [new-t (cond
                   (instant? t-val) (let [requested-time (long-time t-val)]
                                      (and (< requested-time timestamp)
                                           (find-tx tx-manager requested-time)))
                   (int? t-val) (and (< t-val t) t-val)
                   :default (throw (ex-info (str "Unable to retrieve database for datatype " (type t-val))
                                            {:value t-val :type (type t-val)})))]
    (let [bounded-t (min (max 0 new-t) (dec (tx-count tx-manager)))
          {new-ts :timestamp :as tx} (unpack-tx (get-tx tx-manager bounded-t))]
      (->DurableDatabase connection (dgraph/graph-at bgraph tx) bounded-t new-ts))
    database))

(s/defn since* :- (s/maybe DatabaseType)
  "Returns the next database value after the provided t-value.
  If t-val is a transaction number for an older databse, then it returns the next database. If it refers to the current
  database or later, then returns nil (even if more recent databases exist, since this database is stateless.
  If t-val is a timestamp, then it returns the next database after that time, unless the timestamp is at or after
  the timestamp on the current database."
  [{{tx-manager :tx-manager :as connection} :connection
    timestamp :timestamp
    bgraph :bgraph
    t :t :as database} :- DatabaseType
   t-val]
  (letfn [(set-database [tx txid ts]
            (->DurableDatabase connection (dgraph/graph-at bgraph tx) txid ts))
          (db-for [txid]
            (let [{ts :timestamp :as tx} (unpack-tx (get-tx tx-manager txid))]
              (set-database tx txid ts)))]
    ;; check that the database isn't empty
    (when (> (tx-count tx-manager) 0)
      (cond
        ;; look for a since point by timestamp
        (instant? t-val) (let [requested-time (long-time t-val)]
                           (when (< requested-time timestamp)  ;; if at or after the final timestamp, then nil
                             (let [{fts :timestamp :as first-tx} (unpack-tx (get-tx tx-manager 0))]
                               (if (< requested-time fts)       ;; before the first timestamp, so the first commit point
                                 (set-database first-tx 0 fts)
                                 (let [txid (inc (find-tx tx-manager requested-time))]
                                   (db-for txid))))))
        ;; look for a since point by tx ID.
        ;; If it's at or after the time of the latest database, then return nil
        (int? t-val) (when (< t-val t)
                       (let [txid (max 0 (inc t-val))]
                         (db-for txid)))
        :default (throw (ex-info (str "Unable to retrieve database for datatype " (type t-val))
                                 {:value t-val :type (type t-val)}))))))

(s/defn entity* :- (s/maybe {s/Keyword s/Any})
  [{bgraph :bgraph :as database}
   id
   nested? :- s/Bool]
  (if-let [ref (or (and (seq (graph/resolve-triple bgraph id '?a '?v)) id)
                   (ffirst (graph/resolve-triple bgraph '?e :db/ident id)))]
    (reader/ref->entity bgraph ref nested?)))

(defrecord DurableDatabase [connection bgraph t timestamp]
  storage/Database
  (as-of [this t-val] (as-of* this t-val))
  (as-of-t [this] t)
  (since [this t-val] (since* this t-val))
  (since-t [this] t)
  (graph [this] bgraph)
  (entity [this id] (entity* this id false))
  (entity [this id nested?] (entity* this id nested?)))

(s/defn db* :- DatabaseType
  "Returns the most recent database value from the connection."
  [{:keys [name tx-manager grapha] :as connection} :- ConnectionType]
  (let [{:keys [r-spot r-post r-ospt timestamp]} (unpack-tx (latest tx-manager))
        {:keys [spot post ospt] :as g} @grapha
        tx-id (dec (common/tx-count tx-manager))]
    (assert (= r-spot (:root-id spot)))
    (assert (= r-post (:root-id post)))
    (assert (= r-ospt (:root-id ospt)))
    (->DurableDatabase connection g tx-id timestamp)))

(s/defn delete-database*
  [{:keys [grapha] :as connection} :- ConnectionType]
  ;; Delete the graph, which will recursively delete all resources
  (close @grapha)
  (delete! @grapha)
  (reset! grapha nil))

(def DBsBeforeAfter [(s/one DatabaseType "db-before")
                    (s/one DatabaseType "db-after")])

;; Update functions return a Graph, and accept a Graph and an integer
(def UpdateFunction (s/=> GraphType GraphType s/Int))

(s/defn transact-update* :- DBsBeforeAfter
  "Updates a graph according to a provided function. This will be done in a new, single transaction."
  [{:keys [tx-manager grapha] :as connection} :- ConnectionType
   update-fn :- UpdateFunction]
  ;; keep a reference of what the data looks like now
  (let [{:keys [bgraph t timestamp] :as db-before} (db* connection)
        ;; figure out the next transaction number to use
        tx-id (common/tx-count tx-manager)
        ;; do the modification on the graph
        next-graph (update-fn @grapha tx-id)
        ;; step each underlying index to its new transaction point
        graph-after (commit! next-graph)
        ;; get the metadata (tree roots) for all the transactions
        new-timestamp (now)
        tx (assoc (common/get-tx-data graph-after)
                  :timestamp new-timestamp)]
    ;; save the transaction metadata
    (common/append-tx! tx-manager (pack-tx tx))
    ;; update the connection to refer to the latest graph
    (reset! grapha graph-after)
    ;; return the required database values
    [db-before (->DurableDatabase connection graph-after tx-id new-timestamp)]))

(s/defn transact-data* :- DBsBeforeAfter
  "Takes a seq of statements to be asserted, and a seq of statements to be retracted, and applies them each to the graph.
  A new database is created in the process."
  [connection :- ConnectionType
   asserts :- [Triple]
   retracts :- [Triple]]
  (transact-update* connection (fn [graph tx-id] (graph/graph-transact graph tx-id asserts retracts))))

(defrecord DurableConnection [name tx-manager grapha]
  storage/Connection
  (next-tx [this] (common/tx-count tx-manager))
  (db [this] (db* this))
  (delete-database [this] (delete-database* this))
  (transact-update [this update-fn] (transact-update* this))
  (transact-data [this asserts retracts] (transact-data* this asserts retracts)))

(s/defn create-database :- ConnectionType
  "This opens a connection to an existing database by the name of the location for resources.
  If the database does not exist then it is created."
  [name :- s/Str]
  (let [exists? #?(:clj (flat-file/store-exists? name tx-name) :cljc nil)
        tx-manager #?(:clj (flat-file/tx-store name tx-name tx-record-size) :cljc nil)
        tx (latest tx-manager)
        block-graph (dgraph/new-block-graph name (unpack-tx (latest tx-manager)))]
    (->DurableConnection name tx (atom block-graph))))

(s/defn exists? :- s/Bool
  "Deletes this database"
  [uri :- s/Str]
   #?(:clj (flat-file/store-exists? name tx-name) :cljc nil))
