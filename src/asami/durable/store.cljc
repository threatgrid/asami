(ns ^{:doc "The implements the Block storage version of a Graph/Database/Connection"
      :author "Paula Gearon"}
    asami.durable.store
  (:require [asami.storage :as storage :refer [ConnectionType DatabaseType]]
            [asami.graph :as graph]
            [asami.internal :as i :refer [now instant? long-time]]
            [asami.durable.common :as common
             :refer [append-tx! commit! get-tx latest tx-count find-tx close delete!]]
            [asami.durable.macros :as m :include-macros true]
            [asami.durable.pool :as pool]
            [asami.durable.tuples :as tuples]
            [asami.durable.graph :as dgraph]
            [zuko.schema :refer [Triple]]
            [asami.entities.general :refer [GraphType]]
            [asami.entities.reader :as reader]
            [schema.core :as s :include-macros true]
            #?(:clj [asami.durable.flat-file :as flat-file])
            #?(:clj [clojure.java.io :as io]))
  #?(:clj (:import [java.util.concurrent.locks Lock ReentrantLock]
                   [java.nio.channels FileLock])))

#?(:clj (set! *warn-on-reflection* true))

(def tx-name "tx.dat")

;; transactions contain tree roots for the 3 tree indices,
;; the tree root for the data pool,
;; the internal node counter
(def tx-record-size (* 5 common/long-size))

(def TxRecord {(s/required-key :r-spot) (s/maybe s/Int)
               (s/required-key :r-post) (s/maybe s/Int)
               (s/required-key :r-ospt) (s/maybe s/Int)
               (s/required-key :r-pool) (s/maybe s/Int)
               (s/required-key :nr-index-node) (s/maybe s/Int)
               (s/required-key :nr-index-block) (s/maybe s/Int)
               (s/required-key :nr-pool-node) (s/maybe s/Int)
               (s/required-key :nodes) s/Int
               (s/required-key :timestamp) s/Int})

(def TxRecordPacked {(s/required-key :timestamp) s/Int
                     (s/required-key :tx-data) [(s/one s/Int "spot root id")
                                                (s/one s/Int "post root id")
                                                (s/one s/Int "ospt root id")
                                                (s/one s/Int "pool root id")
                                                (s/one s/Int "number of index nodes allocated")
                                                (s/one s/Int "number of index blocks allocated")
                                                (s/one s/Int "number of pool index nodes allocated")
                                                (s/one s/Int "node id counter")]})

(s/defn pack-tx :- TxRecordPacked
  "Packs a transaction into a vector for serialization"
  [{:keys [r-spot r-post r-ospt r-pool nr-index-node nr-index-block nr-pool-node nodes timestamp]} :- TxRecord]
  {:timestamp timestamp :tx-data [(or r-spot 0) (or r-post 0) (or r-ospt 0) (or r-pool 0)
                                  (or nr-index-node 0) (or nr-index-block 0) (or nr-pool-node 0)
                                  nodes]})

(s/defn unpack-tx :- TxRecord
  "Unpacks a transaction vector into a structure when deserializing"
  [{[r-spot r-post r-ospt r-pool
     nr-index-node nr-index-block nr-pool-node nodes] :tx-data
    timestamp :timestamp} :- TxRecordPacked]
  (letfn [(non-zero [v] (and v (when-not (zero? v) v)))]
    {:r-spot (non-zero r-spot)
     :r-post (non-zero r-post)
     :r-ospt (non-zero r-ospt)
     :r-pool (non-zero r-pool)
     :nr-index-node (non-zero nr-index-node)
     :nr-index-block (non-zero nr-index-block)
     :nr-pool-node (non-zero nr-pool-node)
     :nodes nodes
     :timestamp timestamp}))

(s/defn new-db :- TxRecordPacked
  []
  {:timestamp (long-time (now)) :tx-data [0 0 0 0 0 0 0 0]})

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
  (reader/ident->entity bgraph id nested?))

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
  (let [tx (latest tx-manager)
        {:keys [r-spot r-post r-ospt timestamp]} (and tx (unpack-tx tx))
        {:keys [spot post ospt] :as g} @grapha
        tx-id (dec (common/tx-count tx-manager))]
    (assert (= r-spot (:root-id spot)))
    (assert (= r-post (:root-id post)))
    (assert (= r-ospt (:root-id ospt)))
    (->DurableDatabase connection g tx-id timestamp)))

(s/defn delete-database*
  "Delete the graph, which will recursively delete all resources"
  [{:keys [name grapha tx-manager] :as connection} :- ConnectionType]
  (close @grapha)
  (delete! @grapha)
  (reset! grapha nil)
  (close tx-manager)
  (delete! tx-manager)
  #?(:clj (when-let [d (io/file name)]
            (.delete d))
     :cljs true))

(s/defn release*
  "Closes the transaction manager, and the graph, which will recursively close all resources"
  [{:keys [name grapha tx-manager] :as connection} :- ConnectionType]
  (close @grapha)
  (reset! grapha nil)
  (close tx-manager))

(def DBsBeforeAfter [(s/one DatabaseType "db-before")
                    (s/one DatabaseType "db-after")])

;; Update functions return a Graph, and accept a Graph and an integer
(def UpdateFunction (s/=> GraphType GraphType s/Int))

(s/defn transact-update* :- DBsBeforeAfter
  "Updates a graph according to a provided function. This will be done in a new, single transaction."
  [{:keys [tx-manager grapha nodea] :as connection} :- ConnectionType
   update-fn :- UpdateFunction]
  ;; multithreaded environments require exclusive writing for the graph
  ;; this also ensures no writing between the read/write operations of the update-fn
  ;; Locking is required, as opposed to using atoms, since I/O operations cannot be repeated.
  (let [file-lock (volatile! nil)]
    (m/with-lock connection
      (m/with-open* #?(:clj [^FileLock file-lock (common/acquire-lock! tx-manager)]
                       :cljs [file-lock (common/acquire-lock! tx-manager)])
        ;; keep a reference of what the data looks like now
        (let [{:keys [bgraph t timestamp] :as db-before} (db* connection)
              ;; figure out the next transaction number to use
              tx-id (common/tx-count tx-manager)
              ;; do the modification on the graph
              next-graph (update-fn @grapha tx-id)
              ;; step each underlying index to its new transaction point
              graph-after (commit! next-graph)
              ;; get the metadata (tree roots) for all the transactions
              new-timestamp (long-time (now))
              tx (assoc (common/get-tx-data graph-after)
                        :nodes @nodea
                        :timestamp new-timestamp)]
          ;; save the transaction metadata
          (common/append-tx! tx-manager (pack-tx tx))
          ;; update the connection to refer to the latest graph
          (reset! grapha graph-after)
          ;; return the required database values
          [db-before (->DurableDatabase connection graph-after tx-id new-timestamp)])))))

(s/defn transact-data* :- DBsBeforeAfter
  "Removes a series of tuples from the latest graph, and asserts new tuples into the graph.
   Updates the connection to the new graph."
  ([conn :- ConnectionType
    asserts :- [Triple]   ;; triples to insert
    retracts :- [Triple]] ;; triples to remove
   (transact-update* conn (fn [graph tx-id] (graph/graph-transact graph tx-id asserts retracts))))
  ([conn :- ConnectionType
    generator-fn]
   (transact-update* conn
                     (fn [graph tx-id]
                       (let [[asserts retracts] (generator-fn graph)]
                         (graph/graph-transact graph tx-id asserts retracts))))))


(defrecord DurableConnection [name tx-manager grapha nodea lock]
  storage/Connection
  (get-name [this] name)
  (next-tx [this] (common/tx-count tx-manager))
  (db [this] (db* this))
  (delete-database [this] (delete-database* this))
  (release [this] (release* this))
  (transact-update [this update-fn] (transact-update* this update-fn))
  (transact-data [this asserts retracts] (transact-data* this asserts retracts))
  (transact-data [this generator-fn] (transact-data* this generator-fn))
  common/Lockable
  (lock! [this] #?(:clj (.lock ^Lock lock)))
  (unlock! [this] #?(:clj (.unlock ^Lock lock))))

(s/defn db-exists? :- s/Bool
  "Tests if this database exists by looking for the transaction file"
  [store-name :- s/Str]
   #?(:clj (flat-file/store-exists? store-name tx-name) :cljs nil))

(defn- create-lock
  "Creates a lock object for the connection. This is a noop in ClojureScript"
  []
  #?(:clj (ReentrantLock.)))

(s/defn create-database :- ConnectionType
  "This opens a connection to an existing database by the name of the location for resources.
  If the database does not exist then it is created."
  [name :- s/Str]
  (let [ex (db-exists? name)
        tx-manager #?(:clj (flat-file/tx-store name tx-name tx-record-size) :cljs nil)
        ;; initialize new databases with a transaction that indicates an empty store
        _ (when-not ex (common/append-tx! tx-manager (new-db)))
        tx (latest tx-manager)
        unpacked-tx (and tx (unpack-tx tx))
        node-ct (get unpacked-tx :nodes 0)
        node-counter (atom node-ct)
        node-allocator (fn [] (graph/new-node (swap! node-counter inc)))
        ;; the following function is called under locked conditions
        id-checker (fn [id] (when (> id @node-counter) (reset! node-counter id)))
        block-graph (dgraph/new-merged-block-graph name unpacked-tx node-allocator id-checker)]
    (->DurableConnection name tx-manager (atom block-graph) node-counter (create-lock))))

