(ns ^{:doc "A common namespace for protocols and constants that are referenced
           from multiple files and/or between Clojure and ClojureScript files."
      :author "Paula Gearon"}
    asami.durable.common
    (:require #?(:clj [asami.durable.block.file.block-file :as block-file])
              #?(:clj [clojure.java.io :as io])))

(def ^:const long-size "Number of bytes in a Long value"
  #?(:clj Long/BYTES :cljs BigInt64Array.BYTES_PER_ELEMENT))

(def ^:const int-size "Number of bytes in a Integer value"
  #?(:clj Integer/BYTES :cljs Int32Array.BYTES_PER_ELEMENT))

(def ^:const short-size "Number of bytes in a Short value"
  #?(:clj Short/BYTES :cljs Int16Array.BYTES_PER_ELEMENT))

(defprotocol Forceable
  (force! [this] "Ensures that all written data is fully persisted"))

(defprotocol Closeable
  (close [this] "Closes and invalidates all associated resources"))

(defprotocol Transaction
  (rewind! [this] "Revert to the last commit point. Any blocks allocated since the last commit will be invalid.")
  (commit! [this] "Commits all blocks allocated since the last commit. These blocks are now read-only."))

(defprotocol TxStore
  (append! [this tx] "Writes a transaction record. The record is a seq of longs")
  (get-tx [this id] "Retrieves a transaction record by ID")
  (latest [this] "Retrieves the last transaction record")
  (tx-count [this] "Retrieves the count of transaction records")
  (find-tx [this timestamp] "Finds the transaction number for a timestamp"))

(defprotocol DataStorage
  (find-object [pool id] "Retrieves an object by ID")
  (find-id [pool object] "Retrieves an ID for an object")
  (write! [pool object] "Retrieves an ID for an object, writing it if necessary. Returns a pair of the ID and the next version of the store. Idempotent.")
  (at [pool t] "Retrieve the data at a particular transaction."))

(defprotocol Paged
  (refresh! [this] "Refreshes the buffers")
  (read-byte [this offset] "Returns a byte from underlying pages")
  (read-short [this offset] "Returns a short from underlying pages. Offset in bytes.")
  (read-long [this offset] "Returns a long from underlying pages. Offset in bytes. Unlike other data types, these may not straddle boundaries")
  (read-bytes [this offset length] "Reads length bytes and returns as an array.")
  (read-bytes-into [this offset bytes] "Fills a byte array with data from the paged object"))

(defprotocol FlatStore
  (write-object! [this obj] "Writes an object to storage. Returns an ID")
  (get-object [this id] "Reads and object from storage, based on an ID"))

(defprotocol TupleStorage
  (write-tuple! [this tuple] "Adds a tuple to the index")
  (delete-tuple! [this tuple] "Removes a tuple from the index")
  (find-tuple [this tuple] "Finds a specific tuple, returning a co-ordinate"))


(defn create-block-manager
  "Creates a block manager"
  [name manager-name block-size]
  #?(:clj
     (block-file/create-managed-block-file (.getPath (io/file name manager-name)) block-size)))

(defn named-storage
  "A common function for opening storage with a given name. Must be provided with a storage constructor and the name.
  The root id indicates an index root, and may be nil for an empty index."
  [storage-constructor name root-id]
  #?(:clj
     (let [d (io/file name)]
       (if (.exists d)
         (when-not (.isDirectory d)
           (throw (ex-info (str "'" name "' already exists as a file") {:path (.getAbsolutePath d)})))
         (when-not (.mkdir d)
           (throw (ex-info (str "Unable to create directory '" name "'") {:path (.getAbsolutePath d)}))))
       (storage-constructor name root-id))))
