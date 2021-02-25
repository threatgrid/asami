(ns ^{:doc "The implements the Graph over durable storage"
      :author "Paula Gearon"}
    asami.durable.graph
  (:require [asami.storage :as storage]
            [asami.graph :as graph]
            [asami.internal :refer [now instant? long-time]]
            [asami.durable.common :as common :refer [find-tuple write-new-tx-tuple! write-tuple! delete-tuple!
                                                     find-object find-id write! at latest rewind! commit! close]]
            [asami.durable.pool :as pool]
            [asami.durable.tuples :as tuples]
            #?(:clj [asami.durable.flat-file :as flat-file])))

(def spot-name "eavt.idx")
(def post-name "avet.idx")
(def ospt-name "veat.idx")
(def tspo-name "teav.tdx")  ;; a flat file transaction index

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

(defrecord BlockGraph [spot post ospt tspo pool]
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
    (count (graph/resolve-triple this subj pred obj)))

  Transaction
  (rewind! [this]
    (let [spot* (rewind! spot)
          post* (rewind! post)
          ospt* (rewind! ospt)
          ;; tspo does not currently rewind
          pool* (rewind! pool)]
      (->BlockGraph spot* post* ospt* tspo pool*)))

  (commit! [this]
    (let [spot* (commit! spot)
          post* (commit! post)
          ospt* (commit! ospt)
          ;; tspo does not currently commit
          pool* (commit! pool)]
      (->BlockGraph spot* post* ospt* tspo pool*)))


  TxData
  (get-tx-data [this]
    [(:root-id spot) (:root-id post) (:root-id ospt) (:root-id pool)])

  Closeable
  (close [this]
    (close spot)
    (close post)
    (close ospt)
    (close tspo)
    (close pool)))

(defn graph-at
  "Returns a graph based on another graph, but with different set of index roots. This returns a historical graph.
  graph: The graph to base this on. The same index references will be used.
  new-tx: A transaction, containing each of the tree roots for the indices."
  [{:keys [tx spot post ospt tspo pool] :as graph} new-tx]
  (let [{:keys [r-spot r-post r-ospt r-pool]} (unpack-tx new-tx)]
    (->BlockGraph (tuples-at spot r-spot)
                  (tuples-at post r-post)
                  (tuples-at ospt r-ospt)
                  tspo
                  pool)))

(defn new-block-graph
  "Creates a new BlockGraph object, under a given name. If the resources for that name exist, then they are opened.
  If the resources do not exist, then they are created.
  name: the label of the location for the graph resources.
  tx: The transaction record for this graph."
  [name {:keys [r-spot r-post r-ospt r-pool]}]
  (let [spot-index (tuples/create-tuple-index name spot-name r-spot)
        post-index (tuples/create-tuple-index name post-name r-post)
        ospt-index (tuples/create-tuple-index name ospt-name r-ospt)
        tspo-index (flat-file/record-store name tspo-name tuples/tuple-size-bytes)
        data-pool (pool/create-pool name r-pool)]
    (->BlockGraph spot-index post-index ospt-index tspo-index data-pool)))

