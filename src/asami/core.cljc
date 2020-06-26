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
              [zuko.util :as util]
              [zuko.entity.writer :as writer]
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

;; simple type to represent the assertion or removal of data
(deftype Datom [entity attribute value tx-id action])

;; graph is the wrapped graph
;; history is a seq of Databases, excluding this one
;; timestamp is the time the database was created
;; connection is a volatile value that refers to the creating connection
(defrecord Database [graph history timestamp connection])

;; name is the name of the database
;; db is the latest DB
;; history is a list of tuples of Database, including db
(defrecord Connection [name state])

(defn new-empty-connection
  [name empty-gr]
  (let [conn (->Connection name (atom nil))
        db (->Database empty-gr [] (now) conn)]
    (swap! (:state conn) {:db db :history [db]})
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
  (:db @(:state connection)))

(defn assert-data
  [graph data]
  (query/add-to-graph graph data))

(defn retract-data
  [graph data]
  (query/delete-from-graph graph data))

(defn vec-rest
  [s]
  #?(:clj (subvec (vec s) 1)
     :cljs (vec (rest s))))

(defn temp-id?
  [i]
  (and (number? i) (neg? i)))

(defn build-triples
  "Converts a set of transaction data into triples.
  Returns a tuple containing [triples removal-triples tempids]"
  [db data]
  (let [[retractions new-data] (util/divide' #(= :db/retract (first %)) data)
        add-triples (fn [[acc ids] obj]
                      (if (map? obj)
                        (let [[triples new-ids] (writer/ident-map->triples (:graph db) obj)]
                          [(into acc triples) new-ids])
                        (if (and (seqable? obj)
                                 (= 4 (count obj))
                                 (= :db/add (first obj)))
                          (or
                           (if (= (nth obj 2) :db/id)
                             (let [id (nth obj 3)]
                               (if (temp-id? id)
                                 (let [new-id (or (ids id) (gr/new-node))]
                                   [(conj acc (assoc (vec-rest obj) 2 new-id)) (assoc ids (or id new-id) new-id)]))))
                           [(conj acc (vec-rest obj)) ids])
                          (throw (ex-info (str "Bad data in transaction: " obj) {:data obj})))))
        [triples id-map] (reduce add-triples [[] {}] new-data)]
    [triples retractions id-map]))

(defn transact
  [{:keys [name state] :as connection}
   {tx-data :tx-data}]
  (future
    (let [tx-id (count (:history @state))
          as-datom (fn [assert? [e a v]] (->Datom e a v tx-id assert?))
          {:keys [graph history] :as db-before} (:db @state)
          [triples removals tempids] (build-triples db-before tx-data)
          next-graph (-> graph
                         (assert-data triples)
                         (retract-data removals))
          next-connection (->Connection name (atom nil))
          db-after (->Database
                       next-graph
                       (conj history db-before)
                       (now)
                       (atom next-connection))]
      (swap! (:state connection) {:db db-after :history (conj (:history db-after) db-after)})
      {:db-before db-before
       :db-after db-after
       :tx-data (concat
                 (map (partial as-datom true) triples)
                 (map (partial as-datom false) removals))
       :tempids tempids})))

(s/defn q
  [query & inputs]
  (query/query-entry query mem/empty-graph inputs))
