(ns ^{:doc "Data pool with blocks"
      :author "Paula Gearon"}
    asami.durable.pool
  (:require [asami.internal :as internal]
            [asami.durable.common :refer [DataStorage Closeable Forceable Transaction
                                          long-size get-object write-object! get-object
                                          find-tx get-tx append! commit! rewind! force! close]]
            [asami.durable.tree :as tree]
            [asami.durable.flat-file :as flat-file]
            [asami.durable.encoder :refer [to-bytes encapsulate-id]]
            [asami.durable.decoder :refer [type-info long-bytes-compare unencapsulate-id]]
            [asami.durable.block.block-api :refer [get-long get-byte get-bytes put-bytes! put-long!]]
            #?(:clj [asami.durable.block.file.block-file :as block-file])
            #?(:clj [clojure.java.io :as io]))
  #?(:clj
     (:import [java.util Date]
              [java.time Instant])))

(def ^:const index-name "Name of the index file" "idx.bin")

(def ^:const data-name "Name of the data file" "data.bin")

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
    (let [^byte node-type (type-info (get-byte node data-offset))
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

(declare ->ReadOnlyPool)

(defn find*
  [{:keys [data index]} object]
  (let [[header body] (to-bytes object)
        node (tree/find-node index [^byte (type-info (aget header 0)) header body object])]
    (when-not (vector? node)
      (get-long node id-offset-long))))

(defrecord ReadOnlyPool [data index root]
  DataStorage
  (find-object
    [this id]
    (or
     (unencapsulate-id id)
     (get-object data id)))

  (find-id
    [this object]
    (or
     (encapsulate-id object)
     (find* this object)))

  (write! [this object]
    (throw (ex-info "Unsupported Operation" {:cause "Read Only" :operation "write"})))

  (at [this new-root]
    (->ReadOnlyPool data (tree/at index new-root) new-root)))

(defrecord DataPool [data index root]
  DataStorage
  (find-object
    [this id]
    (or
     (unencapsulate-id id)
     (get-object data id)))

  (find-id
    [this object]
    (or
     (encapsulate-id object)
     (find* this object)))

  (write! [this object]
    (if-let [id (encapsulate-id object)]
     [id this]
     (let [[header body :as object-data] (to-bytes object)
           location (tree/find-node index [^byte (type-info (aget header 0)) header body object])]
       (if (or (nil? location) (vector? location))
         (let [id (write-object! data object)
               ;; Note that this writer takes a different format to the comparator!
               ;; That's OK, since this `add` function does not require the location to be found again
               ;; and the writer will format the data correctly
               next-index (tree/add index [object-data id] index-writer location)]
           [id (assoc this :index next-index :root (:root next-index))])
         [(get-long location id-offset) this]))))

  (at [this new-root]
    (->ReadOnlyPool data (tree/at index new-root) new-root))

  Transaction
  (commit! [this]
    (force! data)
    (let [next-index (commit! index)]
      (assoc this :index next-index :root (:root next-index))))

  (rewind! [this]
    (let [next-index (rewind! index)]
      (assoc this :index next-index :root (:root index))))

  Closeable
  (close [this]
    (close index)
    (close data)))

(def data-constructor #?(:clj flat-file/flat-store))

(defn create-block-manager
  "Creates a block manager"
  [name manager-name block-size]
  #?(:clj
     (block-file/create-managed-block-file (.getPath (io/file name manager-name)) block-size)))

(defn open-pool
  "Opens all the resources required for a pool, and returns the pool structure"
  [name root]
  (let [data-store (data-constructor name data-name)
        data-compare (pool-comparator-fn data-store)
        index (tree/new-block-tree (partial create-block-manager name) index-name tree-node-size data-compare root)]
   (->DataPool data-store index root)))

(defn create-pool
  "Creates a datapool object"
  ([name] (create-pool name nil))
  ([name root]
   #?(:clj
      (let [d (io/file name)]
        (if (.exists d)
          (when-not (.isDirectory d)
            (throw (ex-info (str "'" name "' already exists as a file") {:path (.getAbsolutePath d)})))
          (when-not (.mkdir d)
            (throw (ex-info (str "Unable to create directory '" name "'") {:path (.getAbsolutePath d)}))))
        (open-pool name root)))))
