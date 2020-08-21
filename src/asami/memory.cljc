(ns ^{:doc "A storage implementation over in-memory indexing."
      :author "Paula Gearon"}
    asami.memory
    (:require [asami.storage :as storage :refer [ConnectionType DatabaseType]]
              [asami.index :as mem]
              [asami.multi-graph :as multi]
              [asami.graph :as gr]
              [asami.query :as query]
              [zuko.entity.general :as entity :refer [GraphType]]
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


(declare as-of* as-of-t* since* since-t* graph* entity* db* transact-data*)

;; graph is the wrapped graph
;; history is a seq of Databases, excluding this one
;; timestamp is the time the database was created
(defrecord MemoryDatabase [graph history timestamp]
  storage/Database

  (as-of [this t] (as-of* this t))
  (as-of-t [this] (as-of-t* this))
  (since [this t] (since* this t))
  (since-t [this] (since-t* this))
  (graph [this] (graph* this))
  (entity [this id] (entity* this id)))

;; name is the name of the database
;; state is an atom containing:
;; :db is the latest DB
;; :history is a list of tuples of Database, including db
(defrecord MemoryConnection [name state]
  storage/Connection
  (db [this] (db* this))
  (transact-data [this asserts retracts] (transact-data* this asserts retracts)))


(def empty-graph mem/empty-graph)
(def empty-multi-graph multi/empty-multi-graph)


(defn- add-to-graph
  "Adds triples to the graph"
  [graph data]
  (reduce (fn [acc d] (apply gr/graph-add acc d)) graph data))

(defn- delete-from-graph
  "Removes triples from the graph"
  [graph data]
  (reduce (fn [acc d] (apply gr/graph-delete acc d)) graph data))



(defn new-connection
  "Creates a memory Connection object"
  [name gr]
  (let [db (->MemoryDatabase gr [] (now))]
    (->MemoryConnection name (atom {:db db :history [db]}))))

(s/defn db* :- DatabaseType
  "Retrieves the most recent value of the database for reading."
  [connection :- ConnectionType]
  (:db @(:state connection)))

(s/defn as-database :- DatabaseType
  "Creates a Database around an existing Graph.
   graph: The graph to build a database around. "
  [graph :- GraphType]
  (->MemoryDatabase graph [] (now)))

(s/defn as-of* :- DatabaseType
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

(s/defn as-of-t* :- s/Int
  "Returns the as-of point for a database, or nil if none"
  [{history :history :as db} :- DatabaseType]
  (and history (count history)))

(s/defn since* :- (s/maybe DatabaseType)
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

(s/defn since-t* :- s/Int
  "Returns the since point of a database, or nil if none"
  [{history :history :as db} :- DatabaseType]
  (if-not (seq history)
    0
    (count history)))

(s/defn graph* :- GraphType
  "Returns the Graph object associated with a Database"
  [database :- DatabaseType]
  (:graph database))

(defn transact-data*
  "Removes a series of tuples from the latest graph, and asserts new tuples into the graph.
   Updates the connection to the new graph."
  [conn asserts retracts]
  (let [{:keys [graph history] :as db-before} (db* conn)
        next-graph (-> graph
                       (delete-from-graph retracts)
                       (add-to-graph asserts))
        db-after (->MemoryDatabase next-graph (conj history db-before) (now))]
    (reset! (:state conn) {:db db-after :history (conj (:history db-after) db-after)})
    [db-before db-after]))

(s/defn entity* :- {s/Any s/Any}
  "Returns an entity based on an identifier, either the :db/id or a :db/ident if this is available. This eagerly retrieves the entity.
   Objects may be nested, but references to top level objects will be nil in order to avoid loops."
  ;; TODO create an Entity type that lazily loads, and references the database it came from
  [{graph :graph :as db} id]
  (if-let [ref (or (and (seq (gr/resolve-triple graph id '?a '?v)) id)
                   (ffirst (gr/resolve-triple graph '?e :db/ident id)))]
    (reader/ref->entity graph ref)))

