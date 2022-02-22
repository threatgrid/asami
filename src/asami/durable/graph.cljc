(ns ^{:doc "The implements the Graph over durable storage"
      :author "Paula Gearon"}
    asami.durable.graph
    (:require [asami.graph :as graph]
              [asami.common-index :as common-index :refer [?]]
              [asami.durable.common :as common :refer [TxData Transaction Closeable
                                                       find-tuples tuples-at write-new-tx-tuple!
                                                       write-tuple! delete-tuple!
                                                       find-object find-id write! at latest rewind! commit!
                                                       close delete! append! next-id max-long]]
              [asami.durable.common-utils :as common-utils]
              [asami.durable.pool :as pool]
              [asami.durable.tuples :as tuples]
              [asami.durable.resolver :as resolver :refer [get-from-index get-transitive-from-index]]
              #?(:clj [asami.durable.flat-file :as flat-file])
              [asami.durable.block.block-api :as block-api]
              [zuko.node :as node]
              [zuko.logging :as log :include-macros true]))

;; (set! *warn-on-reflection* true)

;; names to use when index files are all separate
(def spot-name "eavt")
(def post-name "avet")
(def ospt-name "veat")
(def tspo-name "teav")  ;; a flat file transaction index

;; names to use when index files are shared
(def index-name "stmtidx.bin")
(def block-name "stmt.bin")

(declare ->BlockGraph)

(defn square [x] (* x x))
(defn cube [x] (* x x x))

(defrecord BlockGraph [spot post ospt tspo pool node-allocator id-checker tree-block-manager tuple-block-manager]
  graph/Graph
  (new-graph
    [this]
    (throw (ex-info "Cannot create a new graph without new storage parameters" {:type "BlockGraph"})))

  (graph-add [this subj pred obj]
    (when (zero? graph/*default-tx-id*)
      (throw (ex-info "Transaction info is required for durable graphs" {:operation :graph-add})))
    (graph/graph-add this subj pred obj graph/*default-tx-id*))
  (graph-add
    [this subj pred obj tx-id]
    (let [[s new-pool] (write! pool subj)
          [p new-pool] (write! new-pool pred)
          [o new-pool] (write! new-pool obj)
          stmt-id (next-id tspo)]
      (if-let [new-spot (write-new-tx-tuple! spot [s p o stmt-id])]
        ;; new statement, so insert it into the other indices and return a new BlockGraph
        (let [new-post (write-tuple! post [p o s stmt-id])
              new-ospt (write-tuple! ospt [o s p stmt-id])
              sid (append! tspo [tx-id s p o])]
          (assert (= stmt-id sid))
          ;; ensure that any imported internal nodes were not outside of range
          (graph/id-check subj id-checker)
          (graph/id-check pred id-checker)
          (graph/id-check obj id-checker)
          ;; return the updated graph
          (assoc this
                 :spot new-spot
                 :post new-post
                 :ospt new-ospt
                 :pool new-pool))
        ;; The statement already existed. The pools SHOULD be identical, but check in case they're not
        (if (identical? pool new-pool)
          this
          (do
            (log/warn "A statement existed that used an element not found in the data pool")
            (assoc this :pool new-pool))))))

  (graph-delete
    [this subj pred obj]
    (or
     (if-let [s (find-id pool subj)]
       (if-let [p (find-id pool pred)]
         (if-let [o (find-id pool obj)]
           (let [[new-spot t] (delete-tuple! spot [s p o])]
             (when t ;; serves as a proxy for (not (identical? spot new-spot))
               (let [[new-post] (delete-tuple! post [p o s t])
                     [new-ospt] (delete-tuple! ospt [o s p t])]
                 ;; the statement stays in tspo
                 (assoc this
                        :spot new-spot
                        :post new-post
                        :ospt new-ospt)))))))
     this))

  (graph-transact
    [this tx-id assertions retractions]
    (common-index/graph-transact this tx-id assertions retractions (volatile! [[] [] {}])))

  (graph-transact
    [this tx-id assertions retractions generated-data]
    (common-index/graph-transact this tx-id assertions retractions generated-data))

  (graph-diff
    [this other]
    (when-not (= (type this) (type other))
      (throw (ex-info "Unable to compare diffs between graphs of different types" {:this this :other other})))
    ;; for every subject, look at the attribute-value sequence in the other graph, and skip that subject if they match
    (let [subjects (map first (find-tuples spot []))]
      (remove (fn [s] (= (find-tuples spot [s]) (find-tuples (:spot other) [s]))) subjects)))

  (resolve-triple
    [this subj pred obj]
    (let [[plain-pred trans-tag] (common-index/check-for-transitive pred)
          get-id (fn [e] (if (symbol? e) e (find-id pool e)))]
      (or
       (if-let [s (get-id subj)]
         (if-let [o (get-id obj)]
           (if plain-pred
             (when-let [p (get-id plain-pred)]
               (log/trace "transitive resolving [" s " " p " " o "]")
               (get-transitive-from-index this trans-tag s p o))
             (when-let [p (get-id pred)]
               (log/trace "resolving [" s " " p " " o "]")
               (get-from-index this s p o)))))
       [])))

  (count-triple
    [this subj pred obj]
    (let [[plain-pred trans-tag] (common-index/check-for-transitive pred)
          get-id (fn [e] (if (symbol? e) e (find-id pool e)))]
      (or
       (if-let [s (get-id subj)]
         (if-let [o (get-id obj)]
           (if plain-pred
             (when-let [p (get-id plain-pred)]
               (log/trace "transitive counting [" s " " p " " o "]")
               (let [varc (count (filter symbol? [s p o]))]
                 ;; make some worst-case estimates rather than actual counts
                 (case varc
                   ;; assuming every use of the predicate is in a chain between the ends
                   0 (resolver/count-from-index this '?s p '?o)
                   1 (if (symbol? p)
                       ;; maximum is a chain of the entire graph between 2 points
                       (resolver/count-from-index this '?s '?p '?o)
                       ;; maximum is a chain of every use of this predicate
                       (resolver/count-from-index this '?s p '?o))
                   2 (if (symbol? p)
                       ;; maximum is an entire subgraph attached to the subject or object
                       (square (resolver/count-from-index this '?s '?p '?o))
                       ;; maximum is every possible connection of nodes that use this predicate
                       ;; factorials are too large, so use cube
                       (cube (resolver/count-from-index this '?s p '?o)))
                   ;; this is every node connected to every node in the same subgraphs
                   ;; cannot be resolved, so give an unreasonable number
                   3 max-long))
               (count (get-transitive-from-index this trans-tag s p o)))
             (when-let [p (get-id pred)]
               (log/trace "counting [" s " " p " " o "]")
               (resolver/count-from-index this s p o)))))
       0)))

  node/NodeAPI
  (data-attribute [_ _] :tg/first)
  (container-attribute [_ _] :tg/contains)
  (new-node [_] (node-allocator))
  (node-id [_ n] (graph/node-id n))
  (node-type? [_ _ n] (graph/node-type? n))
  (find-triple [this [e a v]] (graph/resolve-triple this e a v))

  Transaction
  (rewind! [this]
    (when tree-block-manager (rewind! tree-block-manager))
    (when tuple-block-manager (rewind! tuple-block-manager))
    (let [spot* (rewind! spot)
          post* (rewind! post)
          ospt* (rewind! ospt)
          ;; tspo does not currently rewind
          pool* (rewind! pool)]
      (assoc this
             :spot spot*
             :post post*
             :ospt ospt*
             :pool pool*)))

  (commit! [this]
    (when tree-block-manager (commit! tree-block-manager))
    (when tuple-block-manager (commit! tuple-block-manager))
    (let [spot* (commit! spot)
          post* (commit! post)
          ospt* (commit! ospt)
          ;; tspo does not currently commit
          pool* (commit! pool)]
      (assoc this
             :spot spot*
             :post post*
             :ospt ospt*
             :pool pool*)))


  TxData
  (get-tx-data [this]
    {:r-spot (:root-id spot)
     :r-post (:root-id post)
     :r-ospt (:root-id ospt)
     :r-pool (:root-id pool)
     :nr-index-node (block-api/get-block-count tree-block-manager)
     :nr-index-block (block-api/get-block-count tuple-block-manager)
     :nr-pool-node (block-api/get-block-count pool)})

  Closeable
  (close [this]
    (doseq [resource [spot post ospt tspo pool]]
      (close resource))
    (when tree-block-manager (close tree-block-manager))
    (when tuple-block-manager (close tuple-block-manager)))

  (delete! [this]
    (doseq [resource [spot post ospt tspo pool]]
      (delete! resource))
    (when tree-block-manager (delete! tree-block-manager))
    (when tuple-block-manager (delete! tuple-block-manager))))

(defn graph-at
  "Returns a graph based on another graph, but with different set of index roots. This returns a historical graph.
  graph: The graph to base this on. The same index references will be used.
  new-tx: An unpacked transaction, containing each of the tree roots for the indices."
  [{:keys [spot post ospt] :as graph}
   {:keys [r-spot r-post r-ospt r-pool] :as new-tx}]
  (assoc graph
         :spot (tuples-at spot r-spot)
         :post (tuples-at post r-post)
         :ospt (tuples-at ospt r-ospt)))

(defn new-block-graph
  "Creates a new BlockGraph object, under a given name. If the resources for that name exist, then they are opened.
  If the resources do not exist, then they are created.
  name: the label of the location for the graph resources.
  tx: The transaction record for this graph."
  [name {:keys [r-spot r-post r-ospt r-pool]} node-allocator id-checker]
  (let [spot-index (tuples/create-tuple-index name spot-name r-spot)
        post-index (tuples/create-tuple-index name post-name r-post)
        ospt-index (tuples/create-tuple-index name ospt-name r-ospt)
        tspo-index #?(:clj (flat-file/record-store name tspo-name tuples/tuple-size-bytes) :cljs nil)
        data-pool (pool/create-pool name r-pool nil)]
    (->BlockGraph spot-index post-index ospt-index tspo-index data-pool node-allocator id-checker nil nil)))

(defn new-merged-block-graph
  "Creates a new BlockGraph object, under a given name. If the resources for that name exist, then they are opened.
  If the resources do not exist, then they are created.
  name: the label of the location for the graph resources.
  tx: The transaction record for this graph."
  [name {:keys [r-spot r-post r-ospt r-pool nr-index-node nr-index-block nr-pool-node]} node-allocator id-checker]
  ;; NOTE: Tree nodes blocks must hold the tuples payload and the tree node header
  (let [tree-block-manager (common-utils/create-block-manager name index-name tuples/tree-block-size nr-index-node)
        tuple-block-manager (common-utils/create-block-manager name block-name tuples/block-bytes nr-index-block)
        spot-index (tuples/create-tuple-index-for-managers "SPO" tree-block-manager tuple-block-manager r-spot)
        post-index (tuples/create-tuple-index-for-managers "POS" tree-block-manager tuple-block-manager r-post)
        ospt-index (tuples/create-tuple-index-for-managers "OSP" tree-block-manager tuple-block-manager r-ospt)
        tspo-index #?(:clj (flat-file/record-store name tspo-name tuples/tuple-size-bytes) :cljs nil)
        data-pool (pool/create-pool name r-pool nr-pool-node)]
    (->BlockGraph spot-index post-index ospt-index tspo-index data-pool node-allocator id-checker
                  tree-block-manager tuple-block-manager)))

