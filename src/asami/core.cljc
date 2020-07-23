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
                 :cljs [schema.core :as s :include-macros true])
              #?(:cljs [cljs.reader :as reader]))
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

(defn find-index
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

;; name is the name of the database
;; state is an atom containing:
;; :db is the latest DB
;; :history is a list of tuples of Database, including db
(defrecord Connection [name state])

(defn new-empty-connection
  [name empty-gr]
  (let [db (->Database empty-gr [] (now))]
    (->Connection name (atom {:db db :history [db]}))))

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

(defn as-of
  [{:keys [graph history timestamp] :as db} t]
  (cond
    (instant? t) (if (>= (compare t timestamp) 0)
                   db
                   (nth history
                        (find-index history t #(compare (:timestamp %1) %2))))
    (int? t) (if (>= t (count history))
               db
               (nth history (min (max t 0) (dec (count history)))))))

(defn as-of-t
  "Returns the as-of point, or nil if none"
  [{history :history :as db}]
  (and history (count history)))

(defn since
  "Returns the value of the database since some point t, exclusive
   t can be a transaction number, or Date."
  [{:keys [graph history timestamp] :as db} t]
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

(defn since-t
  "Returns the since point, or nil if none"
  [{history :history :as db}]
  (if-not (seq history)
    0
    (count history)))

(defn assert-data
  "Adds triples to a graph"
  [graph data]
  (query/add-to-graph graph data))

(defn retract-data
  "Removes triples from a graph"
  [graph data]
  (query/delete-from-graph graph data))

(defn update-attribute?
  "Checks if an attribute indicates that it should be updated"
  [a]
  (and (keyword a) (= \' (last (name a)))))

(defn normalize-attribute
  "Converts an updating attribute to its normalized form"
  [a]
  (let [n (name a)]
    (keyword (namespace a) (subs n 0 (dec (count n))))))

(defn contains-updates?
  "Checks if any part of the object is to be updated"
  [obj]
  (or (some update-attribute? (keys obj))
      (some #(and (map? %) (contains-updates? %)) (vals obj))))

(s/defn entity-triples :- [(s/one [Triple] "New triples")
                           (s/one [Triple] "Retractions")
                           (s/one {s/Any s/Any} "New list of ID mappings")]
  [graph :- GraphType
   {id :db/id ident :db/ident :as obj} :- EntityMap
   existing-ids :- {s/Any s/Any}]
  (let [[new-obj removals] (if (contains-updates? obj)
                             (do
                               (when-not (or id ident)
                                 (throw (ex-info "Nodes to be updated must be identified with :db/id or :db/ident")))
                               (let [node-ref (ffirst (if id
                                                        (gr/resolve-triple graph '?r :db/id id)
                                                        (gr/resolve-triple graph '?r :db/ident ident)))
                                     _ (when-not node-ref (throw (ex-info "Cannot update a non-existent node" (select-keys obj [:db/id :db/ident]))))
                                     update-attributes (->> obj
                                                            keys
                                                            (filter update-attribute?)
                                                            (map (fn [a] [a (normalize-attribute a)]))
                                                            (into {}))
                                     clean-obj (->> obj
                                                    (map (fn [[k v :as e]] (if-let [nk (update-attributes k)] [nk v] e)))
                                                    (into {}))
                                     removal-pairs (->> (gr/resolve-triple graph node-ref '?a '?v)
                                                        (filter (comp update-attributes first)))
                                     removals (mapcat (partial writer/existing-triples graph node-ref) removal-pairs)]
                                 [clean-obj removals]))
                             [obj nil])]
    (let [[triples ids] (writer/ident-map->triples graph new-obj existing-ids)]
      [triples removals ids])))

(defn vec-rest
  "Takes a vector and returns a vector of all but the first element. Same as (vec (rest s))"
  [s]
  #?(:clj (subvec (vec s) 1)
     :cljs (vec (rest s))))

(defn temp-id?
  [i]
  (and (number? i) (neg? i)))

(defn build-triples
  "Converts a set of transaction data into triples.
  Returns a tuple containing [triples removal-triples tempids]"
  [{graph :graph :as db} data]
  (let [[retractions new-data] (util/divide' #(= :db/retract (first %)) data)
        add-triples (fn [[acc ids] obj]
                      (if (map? obj)
                        (let [[triples rtriples new-ids] (entity-triples graph obj ids)]
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

#?(:cljs
   (defmacro future
     "Simulates a future by using a delay and immediately forcing it"
     [& body]
     `(force (delay (fn [] ~@body)))))

(defn transact
  [{:keys [name state] :as connection}
   {tx-data :tx-data :as tx-info}]
  (future
    (let [tx-id (count (:history @state))
          as-datom (fn [assert? [e a v]] (->Datom e a v tx-id assert?))
          {:keys [graph history] :as db-before} (:db @state)
          tx-data (or tx-data tx-info) ;; capture the old usage which didn't have an arg map
          [triples removals tempids] (build-triples db-before tx-data)
          next-graph (-> graph
                         (assert-data triples)
                         (retract-data removals))
          db-after (->Database
                    next-graph
                    (conj history db-before)
                    (now))]
      (reset! (:state connection) {:db db-after :history (conj (:history db-after) db-after)})
      {:db-before db-before
       :db-after db-after
       :tx-data (concat
                 (map (partial as-datom true) triples)
                 (map (partial as-datom false) removals))
       :tempids tempids})))

(s/defn entity
  "Returns an entity based on an identifier. This eagerly retrieves the entity. No Loops!"
  ;; TODO create an Entity type that lazily loads, and references the database it came from
  [{graph :graph :as db} id]
  (if-let [ref (or (and (seq (gr/resolve-triple graph id '?a '?v)) id)
                   (ffirst (gr/resolve-triple graph '?e :db/id id))
                   (ffirst (gr/resolve-triple graph '?e :db/ident id)))]
    (reader/ref->entity graph ref)))

(defn graphs-of
  [inputs]
  (map #(if (instance? Database %) (:graph %) %) inputs))

(s/defn q
  [query & inputs]
  (query/query-entry query mem/empty-graph (graphs-of inputs)))
