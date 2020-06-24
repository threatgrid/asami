(ns ^{:doc "A storage implementation over in-memory indexing. Includes full query engine."
      :author "Paula Gearon"}
    asami.core
    (:require [clojure.string :as str]
              [asami.graph :as gr]
              [asami.index :as mem]
              [asami.multi-graph :as multi]
              [asami.query :as query]
              [asami.internal :as internal]
              [naga.store :as store :refer [Storage StorageType]]
              [naga.store-registry :as registry]
              #?(:clj  [schema.core :as s]
                 :cljs [schema.core :as s :include-macros true])
              #?(:clj [clojure.core.cache :as c]))
    #?(:clj (:import [java.util Date])))

(defn now
  "Creates an object to represent the current time"
  []
  #?(:clj (Date.)
     :cljs (js/Date.)))

(defonce databases (atom {}))

(def empty-graph mem/empty-graph)
(def empty-multi-graph multi/empty-multi-graph)

;; graph is the wrapped graph
;; history is a list of Database, excluding this one
;; timestamp is the time the database was created
;; connection is a volatile value that refers to the creating connection
(defrecord Database [graph history timestamp connection])

;; name is the name of the database
;; db is the latest DB
;; history is a list of tuples of Database, including db
(defrecord Connection [name db history])

(defn new-empty-connection
  [name empty-gr]
  (let [db (->Database empty-gr [] (now) (volatile! nil))
        conn (->Connection name db [db])]
    (vswap! (:connection db) conn)
    conn))

(defn parse-uri
  [uri]
  (if (map? uri)
    uri
    (if-let [[_ db-type db-name] (re-find #"asami:([^:]+)://(.+)" uri)]
      {:type db-type
       :name db-name})))

(defn create-database
  "Creates database specified by uri. Returns true if the
   database was created, false if it already exists."
  [uri]
  (boolean
   (if-not (@databases uri)
     (let [{:keys [type name]} (parse-uri uri)]
       (case type
         "mem" (swap! databases assoc uri (new-empty-connection name empty-graph)) 
         "multi" (swap! databases assoc uri (new-empty-connection name empty-multi-graph)) 
         "local" (throw (ex-info "Local Databases not yet implemented" {:type type :name name}))
         (throw (ex-info (str "Unknown graph URI schema" type) {:uri uri :type type :name name})))))))

(defn connect
  "Connects to the specified database, returning a Connection.
  In-memory databases get created if they do not exist already.
  Memory graphs:
  asami:mem://default  A standard graph
  asami:mem://multi  A multigraph"
  [uri]
  (if-let [conn (@databases uri)]
    conn
    (do
      (create-database uri)
      (@databases uri))))

(defn db
  "Retrieves a value of the database for reading."
  [connection]
  (:db connection))

(defn assert-data
  [graph data]
  (query/add-to-graph graph data))

(defn retract-data
  [graph data]
  (query/delete-from-graph graph data))

(defn build-triples
  [db data]
  ;; generate triples
  )

(defn transact
  [connection tx-data]
  (future
    (let [{:keys [graph history] :as db-before} (db connection)
          [tx-data tempids] (build-triples db-before tx-data)
          next-graph (assert-data graph tx-data)
          db-after (->Database
                       next-graph
                       (conj history db-before)
                       (now)
                       (volatile! nil))
          next-connection (->Connection (:name connection) db-after (conj (:history db-after) db-after))]
      (vswap! (:connection db-after) next-connection)
      {:db-before db-before
       :db-after (-> db-before
                     (assoc :graph next-graph)
                     (update :history conj next-graph))
       :tx-data tx-data
       :tempids tempids})))

(s/defn q
  [query & inputs]
  (query/query-entry query mem/empty-graph inputs))
