(ns ^{:doc "Data pool on disk"
      :author "Paula Gearon"}
    asami.durable.pool
  (:require [clojure.java.io :as io]
            [asami.durable.stores :refer [DataPool]]
            [schema.core :as s])
    (:import [java.io RandomAccessFile ObjectOutputStream FileOutputStream]))

(def ^:const index-name "Name of the index file" "idx.bin")

(def ^:const data-name "Name of the data file" "data.bin")

(def ^:const tx-name "Name of the transaction file" "tx.bin")

(declare avl-file)
(declare new-file-data-pool)

;; tx-id: offset for the current transaction, in monotonically increasing order
;; root: Block ID for the root of the index tree in the current transaction
;; tail: offset of the end of the raw data file
;; index: The index object, with the root provided as an ID
;; data-writer: append-only file handle to write data
;; data-reader: random access handle to read data
;; tx-writer: append-only file stream to write transactions
;; tx-reader: random access handle to read data
(defrecord FileDataPool [tx-id root tail index data-writer data-reader tx-writer tx-reader]
  DataPool
  (find-object [pool id] "Retrieves an object by ID")
  (find-id [pool object] "Retrieves an ID for an object")
  (write [pool object] "Retrieves an ID for an object, writing it if necessary. Idempotent.")
  (commit [pool] "Ensures that all pending writes are on disk and immutable. Returns transaction ID.")
  (rollback [pool] "Resets to the point of the last commit.")
  (at [pool tx]
    (let [tx-length (.length tx-reader)
          max-tx (/ tx-length tx-size)]
      (assert (zero? (mod tx-length tx-size)) "Data Pool transaction file corrupted")
      (cond
        (= (inc tx) max-tx) pool
        (or (>= tx max-tx) (neg? tx)) (throw (ex-info "Transaction of range" {:tx tx :max max-tx}))
        :default (new-file-data-pool tx index nil data-reader nil tx-reader)))))

(defn new-file-data-pool
  [tx-id index data-writer data-reader tx-writer tx-reader]
  (let [root _
        tail _]
    (->FileDataPool tx-id root tail index data-writer data-reader tx-writer tx-reader)))

(defn open-pool
  "Opens all the resources required for a pool, and returns the pool structure"
  [name]
  (let [idx (avl-file (io/file name index-name))
        data-f (io/file name data-name)
        data-writer (ObjectOutputStream. (FileOutputStream. data-f))
        data-reader (RandomAccessFile. data-f "r")
        tx-f (io/file name tx-name)
        tx-writer (ObjectOutputStream. (FileOutputStream. tx-f))
        tx-reader (RandomAccessFile. tx-f "r")
        tx-length (.length tx-reader)]
    (when-not (zero? (mod tx-size tx-size))
      (throw (ex-info "Bad transaction file!" {:length tx-length})))
   (new-file-data-pool (/ tx-length tx-size) idx data-writer data-reader tx-writer tx-reader)))

(defn create-pool
  "Creates a datapool object"
  [name]
  (let [d (io/file name)]
    (if (.exists d)
      (when-not (.isDirectory d)
        (throw (ex-info (str "'" name "' already exists as a file") {:path (.getAbsolutePath d)})))
      (when-not (.mkdir d)
        (throw (ex-info (str "Unable to create directory '" name "'") {:path (.getAbsolutePath d)}))))
    (open-pool name)))
