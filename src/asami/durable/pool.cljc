(ns ^{:doc "Data pool with blocks"
      :author "Paula Gearon"}
    asami.durable.pool
  (:require [asami.durable.common :refer [DataStorage long-size get-object find-node write-object! get-object
                                          find-tx get-tx]]
            [asami.durable.tree :as tree]
            [asami.durable.block-api :refer [get-long get-byte]]))

(def ^:const index-name "Name of the index file" "idx.bin")

(def ^:const data-name "Name of the data file" "data.bin")

(def ^:const tx-name "Name of the transaction file" "tx.bin")

(def ^:const tree-node-size "Number of bytes available in the index nodes" (* 2 long-size))

(def ^:const data-offset 0)

(def ^:const id-offset 1)

(defn index-writer
  [node [object id]]
  ;; TODO
  )

(defn pool-comparator-fn
  "Returns a function that can compare data to what is found in a node"
  [data-store]
  (fn [data node]
    (let [type-into (get-byte node 0)]
      ;; TODO
      )))

(defn as-millis
  "If t represents a time value, then return it as milliseconds"
  [t]
  #?(:clj
     (cond
       (instance? Instant t) (.toEpochMilli ^Instant t)
       (instance? Date t) (.getTime ^Date t))
     :cljs
     (when (instance? js/Date t)
       (.getTime t))))

(defn at*
  [{:keys [tx data index]} t]
  (let [id (if-let [timestamp (as-millis t)]
             (find-tx tx timestamp)
             t)
        tx-data (:tx-data (get-tx tx id))]
    (->ReadOnlyPool tx data (tree/at index tx))))

(defrecord ReadOnlyPool [tx data index]
  DataStorage
  (find-object
    [this id]
    (get-object data id))

  (find-id
    [this object]
    (let [node (tree/find-node index object)]
      (when-not (vector? node)
        (get-long node id-offset))))

  (write! [this object]
    (throw (ex-info "Unsupported Operation" {:cause "Read Only" :operation "write"})))

  (at [this t]
    (at* this t)))

(defrecord DataPool [tx data index]
  DataStorage
  (find-object
    [this id]
    (get-object data id))

  (find-id
    [this object]
    (let [node (find-node index object)]
      (when-not (vector? node)
        (get-long node id-offset))))

  (write! [this object]
    (let [node (find-node index object)]
      (if (vector? node)
        (let [id (write-object! data object)]
          (add index [object id] index-writer))
        (get-long node id-offset))))

  (at [this t]
    (at* this t))

  Transaction
  (commit! [this]
    (force! data)
    (let [next-index (commit! index)
          root (:root index)]
      (append! tx {:timestamp (now) :tx-data root})
      (assoc this :index next-index)))

  (rewind! [this]
    (assoc this :index (rewind! index))))

(defn open-pool
  "Opens all the resources required for a pool, and returns the pool structure"
  [name]
  (let [tx-store (flat-file/tx-store name tx-name)
        data-store (flat-file/flat-store name data-name)
        data-compare (pool-comparator-fn data-store)
        index (tree/new-block-tree create-block-manager index-name (* 2 long-size) data-compare)]
   (->DataPool tx-store data-store index)))

(defn create-pool
  "Creates a datapool object"
  [name]
  #?(:clj
     (let [d (io/file name)]
       (if (.exists d)
         (when-not (.isDirectory d)
           (throw (ex-info (str "'" name "' already exists as a file") {:path (.getAbsolutePath d)})))
         (when-not (.mkdir d)
           (throw (ex-info (str "Unable to create directory '" name "'") {:path (.getAbsolutePath d)}))))
       (open-pool name))))
