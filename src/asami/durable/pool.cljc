(ns ^{:doc "Data pool with blocks"
      :author "Paula Gearon"}
    asami.durable.pool
  (:require [asami.durable.common :refer [DataStorage long-size get-object find-node write-object! get-object
                                          find-tx get-tx]]
            [asami.durable.tree :as tree]
            [asami.durable.encoder :refer [to-bytes encapsulation-id]]
            [asami.durable.decoder :refer [type-info long-bytes-compare]]
            [asami.durable.block-api :refer [get-long get-byte put-bytes! put-long!]]
            #?(:clj [clojure.java.io :as io]))
  #?(:clj
     (:import [java.util Date]
              [java.time Instant])))

(def ^:const index-name "Name of the index file" "idx.bin")

(def ^:const data-name "Name of the data file" "data.bin")

(def ^:const tx-name "Name of the transaction file" "tx.bin")

(def ^:const tree-node-size "Number of bytes available in the index nodes" (* 4 long-size))

(def ^:const data-offset 0)

(def ^:const id-offset-long 3)

(def ^:const id-offset (* id-offset-long long-size))

(def ^:const payload-len (- id-offset data-offset))

(defn index-writer
  [node [[header body] id]]
  (let [hdr-len (count header)
        remaining (- payload-len hdr-len)]
    (put-bytes! node data-offset hdr-len header)
    (when (> remaining 0)
      (put-bytes! node hdr-len (min remaining (count body)) body))
    (put-long! node id-offset-long id)))

(defn pool-comparator-fn
  "Returns a function that can compare data to what is found in a node"
  [data-store]
  (fn [[type-byte header body object] node]
    (let [node-type (type-info (get-byte node data-offset))
          c (compare type-byte node-type)]
      (if (zero? c)
        (let [nc (long-bytes-compare type-byte header body object (get-bytes node data-offset long-size))]
          (if (zero? nc)
            ;; There is an optimization option here if one of the strings is shorter than the
            ;; node payload length and matches the header of the other string, then they match
            ;; and this next step is performed. Instead, in this case a +/- 1 can be returned.
            (let [stored-data (get-object data-store (get-long node id-offset-long))]
              (compare object stored-data))
            nc))
        c))))

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

(defn find*
  [{:keys [data index]} object]
  (let [[header body] (to-bytes object)
        node (tree/find-node index [(type-info (aget header 0)) header body object])]
    (when-not (vector? node)
      (get-long node id-offset))))

(defrecord ReadOnlyPool [tx data index]
  DataStorage
  (find-object
    [this id]
    (get-object data id))

  (find-id
    [this object]
    (find* this object))

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
    (find* this object))

  (write! [this object]
    (let [[header body] (to-bytes object)
          node (tree/find-node index [(type-info (aget header 0)) header body object])]
      (if (vector? node)
        (let [id (or (encapsulated-id object)
                     (write-object! data object))]
          (add index [object-data id] index-writer))
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

(def tx-constructor #?(:clj flat-file/tx-store))

(def data-constructor #?(:clj flat-file/flat-store))

(defn create-block-manager
  "Creates a block manager"
  [name managername block-size]
  #?(:clj
     (create-managed-block-file (.getPath (io/file name manager-name)) block-size)))

(defn open-pool
  "Opens all the resources required for a pool, and returns the pool structure"
  [name]
  (let [tx-store (tx-constructor name tx-name)
        data-store (data-constructor name data-name)
        data-compare (pool-comparator-fn data-store)
        index (tree/new-block-tree (partial create-block-manager name) index-name (* 2 long-size) data-compare)]
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
