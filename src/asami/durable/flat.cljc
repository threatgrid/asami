(ns ^{:doc "Cross platform protocol for the flat storage. This stores data, returning an ID. The data can be retrieved with this ID"
      :author "Paula Gearon"}
    asami.durable.flat)

(defprotocol FlatStore
  (write-object! [this obj] "Writes an object to storage. Returns an ID")
  (get-object [this id] "Reads and object from storage, based on an ID")
  (force! [this] "Ensures that all written data is fully persisted"))
