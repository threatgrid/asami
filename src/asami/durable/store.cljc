(ns ^{:doc "The implements the Block storage version of a Graph/Database/Connection"
      :author "Paula Gearon"}
    asami.durable.store
  (:require [asami.storage :as storage]
            [asami.graph :as graph]
            [asami.durable.common :as common :refer [find-tuple write-new-tx-tuple! write-tuple! delete-tuple!
                                                     find-object find-id write! at latest]]
            [asami.durable.pool :as pool]
            [asami.durable.tuples :as tuples]
            #?(:clj [asami.durable.flat-file :as flat-file])))

(def tx-name "tx.dat")
(def spot-name "eavt.idx")
(def post-name "avet.idx")
(def ospt-name "veat.idx")
(def tspo-name "teav.tdx")  ;; a flat file transaction index

;; transactions contain tree roots for the 3 tree indices,
;; the tree root for the data pool
;; and a transaction timestamp
(def tx-record-size (* 5 common/long-size))

(defn pack-tx
  "Packs a transaction into a vector for serialization"
  [{:keys [r-spot r-post r-ospt r-tspo r-pool]}]
  [r-spot r-post r-ospt r-tspo r-pool])

(defn unpack-tx
  "Unpacks a transaction vector into a structure when deserializing"
  [{[r-spot r-post r-ospt r-tspo r-pool] :tx-data}]
  {:r-spot r-spot :r-post r-post :r-ospt r-ospt :r-tspo r-tspo :r-pool r-pool})

(defmulti get-from-index
  "Lookup an index in the graph for the requested data.
   Returns a sequence of unlabelled bindings. Each binding is a vector of binding values."
  common/simplify)

(def v2 (fn [dp t] (vector (find-object dp (nth t 2)))))
(def v12 (fn [dp t] (vector
                     (find-object dp (nth t 1))
                     (find-object dp (nth t 2)))))
(def v21 (fn [dp t] (vector
                     (find-object dp (nth t 2))
                     (find-object dp (nth t 1)))))

;; Extracts the required index (idx), and looks up the requested fields.
;; If an embedded index is pulled out, then this is referred to as edx.
(defmethod get-from-index [:v :v :v]
  [{idx :spot dp :pool} s p o]
  (if (seq (find-tuple idx [s p o])) [[]] []))

(defmethod get-from-index [:v :v  ?]
  [{idx :spot dp :pool} s p o]
  (map (partial v2 dp) (find-tuple idx [s p])))

(defmethod get-from-index [:v  ? :v]
  [{idx :ospt dp :pool} s p o]
  (map (partial v2 dp) (find-tuple idx [o s])))

(defmethod get-from-index [:v  ?  ?]
  [{idx :spot dp :pool} s p o]
  (map (partial v12 dp) (find-tuple idx [s])))

(defmethod get-from-index [ ? :v :v]
  [{idx :post dp :pool} s p o]
  (map (partial v2 dp) (find-tuple idx [p o])))

(defmethod get-from-index [ ? :v  ?]
  [{idx :post dp :pool} s p o]
  (map (partial v21 dp) (find-tuple idx [p])))

(defmethod get-from-index [ ?  ? :v]
  [{idx :ospt dp :pool} s p o]
  (map (partial v12 dp) (find-tuple idx [o])))

(defmethod get-from-index [ ?  ?  ?]
  [{idx :spot dp :pool} s p o]
  (map #(mapv (partial find-object dp) %)
       (find-tuple idx [s p o])))

(declare ->BlockGraph)

(defrecord BlockGraph [tx spot post ospt tspo pool]
    graph/Graph
  (new-graph
    [this]
    (throw (ex-info "Cannot create a new graph without new storage parameters" {:type "BlockGraph"})))

  (graph-add
    [this subj pred obj tx-id]
    (let [[s new-pool] (write! pool subj)
          [p new-pool] (write! pool pred)
          [o new-pool] (write! pool obj)
          stmt-id (next-id tspo)]
      (if-let [new-spot (write-new-tx-tuple! spot [s p o stmt-id])]
        ;; new statement, so insert it into the other indices and return a new BlockGraph
        (let [new-post (write-tuple! post [p o s stmt-id])
              new-ospt (write-tuple! ospt [o s p stmt-id])
              new-tspo (write-tuple! tspo [tx-id s p o])]
          (->BlockGraph new-spot new-post new-ospt new-tspo new-pool))
        ;; The statement already existed. The pools SHOULD be identical, but check in case they're not
        (if (identical? pool new-pool)
          this
          (assoc this :pool new-pool)))))

  (graph-delete
    [this subj pred obj]
    (let [new-spot (delete-tuple! spot [s p o])]
      (if (identical? spot new-spot)
        this
        (let [[new-post] (delete-tuple! post [p o s t])
              [new-ospt] (delete-tuple! ospt [o s p t])]
          ;; the statement stays in tspo
          (->BlockGraph new-spot new-post new-ospt new-tspo)))))

  (graph-transact
    [this tx-id assertions retractions]
    (as-> this graph
      (reduce (fn [acc [s p o]] (graph/graph-delete acc s p o)) graph retractions)
      (reduce (fn [acc [s p o]] (graph/graph-add acc s p o tx-id)) graph assertions)))

  (graph-diff
    [this other]
    (when-not (= (type this) (type other))
      (throw (ex-info "Unable to compare diffs between graphs of different types" {:this this :other other})))
    ;; for every subject, look at the attribute-value sequence in the other graph, and skip that subject if they match
    (let [subjects (map first (find-tuple spot []))]
      (remove (fn [s] (= (find-tuple spot [s]) (find-tuple (:spot other) [s]))) subjects)))

  (resolve-triple
    [this subj pred obj]
    (if-let [[plain-pred trans-tag] (common/check-for-transitive pred)]
        ;; TODO: (common/get-transitive-from-index this trans-tag subj plain-pred obj)
      (throw (ex-info "Transitive resolutions not yet supported" {:pattern [subj pred obj]}))
      (get-from-index this subj pred obj)))

  (count-triple
    [this subj pred obj]
    ;; TODO count by node to make this faster for large numbers
    (count (graph/resolve-triple this subj pred obj))))

(defn new-block-graph
  [name tx]
  (let [{:keys [r-spot r-post r-ospt r-tspo r-pool]} (unpack-tx (latest tx))
        spot-index (tuples/create-tuple-index name spot-name r-spot)
        post-index (tuples/create-tuple-index name post-name r-post)
        ospt-index (tuples/create-tuple-index name ospt-name r-ospt)
        tspo-index (flat-file/create-record-store name tspo-name tuples/tuple-size-bytes)
        data-pool (pool/create-pool name r-pool)]
    (->BlockGraph tx spot-index post-index ospt-index tspo-index data-pool)))



(defrecord DurableConnection [name tx-manager bgraph]
  storage/Connection
  (next-tx [this] (common/tx-count tx-manager))
  (db [this] (db* this))
  (delete-database [this] (delete-database* this))
  (transact-update [this] (transact-update* this))
  (transact-data [this asserts retracts] (transact-data* this asserts retracts)))

(defn db*
  [{:keys [name tx-manager bgraph]}]
  (let [{:keys [r-spot r-post r-ospt r-tspo r-pool]} (unpack-tx (latest tx-manager))
        #_graph #_(->BlockGraph tx-manager
                            (at bgraph))]
    ))

(defn create-database
  [name]
  (let [exists? (flat-file name tx-name)
        tx #?(:clj (flat-file/tx-store name tx-name) :cljc nil)
        block-graph (new-block-graph name tx)]
    (->DurableConnection name tx block-graph)))

