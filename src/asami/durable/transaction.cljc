(ns ^{:doc "Describes transaction operations"
      :author "Paula Gearon"}
    asami.durable.transaction)

(defprotocol Transaction
  (rewind! [this] "Revert to the last commit point. Any blocks allocated since the last commit will be invalid.")
  (commit! [this] "Commits all blocks allocated since the last commit. These blocks are now read-only.")
  (close [this] "Releases all resources currently in use by this object."))
