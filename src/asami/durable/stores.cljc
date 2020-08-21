(ns ^{:doc "Protocols for storing data"
      :author "Paula Gearon"}
    asami.durable.stores)

(defprotocol DataPool
  (find-object [pool id] "Retrieves an object by ID")
  (find-id [pool object] "Retrieves an ID for an object")
  (write [pool object] "Retrieves an ID for an object, writing it if necessary. Idempotent.")
  (commit [pool] "Ensures that all pending writes are on disk and immutable. Returns transaction ID.")
  (rollback [pool] "Resets to the point of the last commit.")
  (at [pool t] "Retrieve the data at a particular transaction."))
