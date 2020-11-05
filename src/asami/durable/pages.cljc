(ns ^{:doc "Common protocols for paged reading of data"
      :author "Paula Gearon"}
    asami.durable.pages)

(defprotocol Paged
  (refresh! [this] "Refreshes the buffers")
  (read-byte [this offset] "Returns a byte from underlying pages")
  (read-short [this offset] "Returns a short from underlying pages. Offset in bytes.")
  (read-long [this offset] "Returns a long from underlying pages. Offset in bytes. Unlike other data types, these may not straddle boundaries")
  (read-bytes [this offset length] "Reads length bytes and returns as an array.")
  (read-bytes-into [this offset bytes] "Fills a byte array with data from the paged object"))

