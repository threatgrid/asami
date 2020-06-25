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
  (let [[retractions new-data] (divide' #(= :db/retract (first %)) data)
        add-triples (fn [[acc ids] obj]
                      (if (map? obj)
                        (let [id (:db/id obj)
                              [obj ids] (if (or (temp-id? id) (nil? id))
                                          (let [new-id (or (ids id) (new-node))]
                                            [(assoc obj :db/id new-id) (assoc ids (or id new-id) new-id)])
                                          [obj ids])
                              triples (ident-map->triples graph obj)]
                          [(into acc triples) ids])
                        (if (and (seqable? obj)
                                 (= 4 (count obj))
                                 (= :db/add (first obj)))
                          (if (= (nth obj 2) :db/id)
                            (let [id (nth obj 3)]
                              (if (temp-id? id)
                                (let [new-id (or (ids id) (new-node))]
                                  [(conj acc (assoc (vec-rest obj) 2 new-id)) (assoc ids (or id new-id) new-id)])
                                [(conj acc (vec-rest obj)) ids]))
                            [(conj acc (vec-rest obj)) ids])
                          (throw (ex-info (str "Bad data in transaction: " obj) {:data obj})))))
        [triples id-map] (reduce add-triples [[] {}] new-data)]
    [triples retractions id-map]))

(defn transact
  [connection
   {tx-data :tx-data}]
  (future
    (let [tx-id (count (:history connection))
          as-datom (fn [assert? [e a v]] (->Datom e a v tx-id assert?))
          {:keys [graph history] :as db-before} (db connection)
          [triples removals tempids] (build-triples db-before tx-data)
          next-graph (-> graph
                         (assert-data triples)
                         (retract-data removals))
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
       :tx-data (concat
                 (as-datoms true triples)
                 (as-datoms false removals))
       :tempids tempids})))

(s/defn q
  [query & inputs]
  (query/query-entry query mem/empty-graph inputs))
