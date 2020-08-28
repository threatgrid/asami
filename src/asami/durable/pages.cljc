(ns ^{:doc "Common protocols for paged reading of data"
      :author "Paula Gearon"}
    asami.durable.pages)

(defprotocol Paged
  (refresh! [this] "Refreshes the buffers")
  (read-byte [this offset] "Returns a byte from underlying pages")
  (read-short [this offset] "Returns a short from underlying pages. Offset in bytes.")
  (read-bytes [this offset bytes] "Fills a byte array with data from the paged object"))
