(ns ^{:doc "Describes transaction operations"
      :author "Paula Gearon"}
    asami.durable.transaction)

(defprotocol Closeable
  (close [this] "Closes and invalidates all associated resources"))

(defprotocol Transaction
  (rewind! [this] "Revert to the last commit point. Any blocks allocated since the last commit will be invalid.")
  (commit! [this] "Commits all blocks allocated since the last commit. These blocks are now read-only."))

(defprotocol TxStore
  (append! [this tx] "Writes a transaction record")
  (get-tx [this id] "Retrieves a transaction record by ID")
  (latest [this] "Retrieves the last transaction record")
  (tx-count [this] "Retrieves the count of transaction records")
  (find-tx [this timestamp] "Finds the transaction number for a timestamp")
  (force! [this] "If writing can be delayed, then this ensures that it is complete"))
