(ns ^{:doc "Data pool with blocks"
      :author "Paula Gearon"}
    asami.durable.pool
    (:require [asami.durable.common :refer [DataStorage Closeable Forceable Transaction
                                            long-size get-object write-object! get-object
                                            find-tx get-tx append! commit! rewind! force! close delete!]]
              [asami.durable.common-utils :as common-utils]
              [asami.durable.tree :as tree]
              [asami.durable.encoder :as encoder :refer [to-bytes]]
              [asami.durable.decoder :as decoder :refer [type-info long-bytes-compare]]
              [asami.durable.block.block-api :refer [get-long get-byte get-bytes put-byte! put-bytes! put-long! get-id]]
              [asami.durable.cache :refer [lookup hit miss lru-cache-factory]]
              #?(:clj [asami.durable.flat-file :as flat-file])))

;; (set! *warn-on-reflection* true)

(def ^:const index-name "Name of the index file" "idx.bin")

(def ^:const data-name "Name of the data file" "data.bin")

(def ^:const data-offset 0)

(def ^:const id-offset-long 2)

(def ^:const id-offset (* id-offset-long long-size))

(def ^:const payload-len (- id-offset data-offset))

(def ^:const tree-node-size "Number of bytes used in the index nodes" (* (inc id-offset-long) long-size))

(defn get-object-ref
  [node]
  (get-long node id-offset-long))

(defn index-writer
  [node [[header body] id]]
  (let [remaining (dec payload-len)]
    (put-byte! node data-offset (aget ^bytes header 0))
    (when (> remaining 0)
      (put-bytes! node 1 (min remaining (alength ^bytes body)) body))
    (put-long! node id-offset-long id)))

(defn pool-comparator-fn
  "Returns a function that can compare data to what is found in a node"
  [data-store]
  (fn [[type-byte header body object] node]
    (let [node-type (type-info (get-byte node data-offset))
          c (compare type-byte node-type)]
      (if (zero? c)
        (let [nc (long-bytes-compare (byte type-byte) header body object (get-bytes node data-offset payload-len))]
          (if (zero? nc)
            ;; There is an optimization option here if one of the strings is shorter than the
            ;; node payload length and matches the header of the other string, then they match
            ;; and this next step is performed. Instead, in this case a +/- 1 can be returned.
            (let [stored-data (get-object data-store (get-object-ref node))]
              (compare object stored-data))
            nc))
        c))))

(declare ->ReadOnlyPool)

(defn find*
  [{:keys [data index]} object]
  (let [[header body] (to-bytes object)
        node (tree/find-node index [^byte (type-info (aget ^bytes header 0)) header body object])]
    (when (and node (not (vector? node)))
      (get-object-ref node))))

(defrecord ReadOnlyPool [data index root-id cache]
  DataStorage
  (find-object
    [this id]
    (or
     (decoder/unencapsulate-id id)
     (get-object data id)))

  (find-id
    [this object]
    (or
     (encoder/encapsulate-id object)
     (find* this object)))

  (write! [this object]
    (throw (ex-info "Unsupported Operation" {:cause "Read Only" :operation "write"})))

  (at [this new-root-id]
    (->ReadOnlyPool data (tree/at index new-root-id) new-root-id)))

(defrecord DataPool [data index root-id cache]
  DataStorage
  (find-object
    [this id]
    (or
     (decoder/unencapsulate-id id)
     (get-object data id)))

  (find-id
    [this object]
    (or
     (encoder/encapsulate-id object)
     (find* this object)))

  (write! [this object]
    (if-let [id (encoder/encapsulate-id object)]
      [id this]
      (if-let [id (lookup @cache object)]
        (do
          (swap! cache hit object)
          [id this])
        (let [[header body :as object-data] (to-bytes object)
              location (tree/find-node index [^byte (type-info (aget ^bytes header 0)) header body object])]
          (if (or (nil? location) (vector? location))
            (let [id (write-object! data object)
                  ;; Note that this writer takes a different format to the comparator!
                  ;; That's OK, since this `add` function does not require the location to be found again
                  ;; and the writer will format the data correctly
                  next-index (tree/add index [object-data id] index-writer location)]
              (swap! cache miss object id)
              [id (assoc this :index next-index :root-id (get-id (:root next-index)))])
            (let [id (get-object-ref location)]
              (swap! cache miss object id)
              [id this]))))))

  (at [this new-root-id]
    (->ReadOnlyPool data (tree/at index new-root-id) new-root-id cache))

  Transaction
  (commit! [this]
    (force! data)
    (let [{r :root :as next-index} (commit! index)]
      ;; root can be nil if only small values have been stored
      (assoc this :index next-index :root-id (and r (get-id r)))))

  (rewind! [this]
    (let [{r :root :as next-index} (rewind! index)]
      ;; root can be nil if only small values have been stored
      (assoc this :index next-index :root-id (and r (get-id r)))))

  Closeable
  (close [this]
    (close index)
    (close data))

  (delete! [this]
    (delete! index)
    (delete! data)))

(def data-constructor #?(:clj flat-file/flat-store))

(def ^:const encap-cache-size 1024)

(defn open-pool
  "Opens all the resources required for a pool, and returns the pool structure"
  [name root-id]
  (let [data-store (data-constructor name data-name)
        data-compare (pool-comparator-fn data-store)
        index (tree/new-block-tree (partial common-utils/create-block-manager name)
                                   index-name tree-node-size data-compare root-id)
        encap-cache (atom (lru-cache-factory {} :threshold encap-cache-size))]
   (->DataPool data-store index root-id encap-cache)))

(defn create-pool
  "Creates a datapool object"
  ([name] (create-pool name nil))
  ([name root-id] (common-utils/named-storage open-pool name root-id)))
