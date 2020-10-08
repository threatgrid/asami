(ns ^{:doc "Defines the protocols for allocating an manipulating blocks"
      :author "Paula Gearon"}
    asami.durable.block.block-api)

(defprotocol Block
  "An abstraction over a block of raw binary data of fixed length"
  (get-id [this] "Returns the ID of the block.")
  (get-byte [this offset] "Returns the byte at a given offset within the block.")
  (get-int [this offset] "Returns the integer at a given offset within the block. Offset is in Integers.")
  (get-long [this offset] "Returns the long at a given offset within the block. Offset is in Longs.")
  (get-bytes [this offset len] "Returns the bytes at a given offset within the block.")
  (get-ints [this offset len] "Returns the ints at a given offset within the block. Offset is in Integers.")
  (get-longs [this offset len] "Returns the longs at a given offset within the block. Offset is in Longs.")
  (put-byte! [this offset value] "Modifies the byte at a given offset within the block.")
  (put-int! [this offset value] "Modifies the integer at a given offset within the block. Offset is in Integers.")
  (put-long! [this offset value] "Modifies the long at a given offset within the block. Offset is in Longs.")
  (put-bytes! [this offset len values] "Modifies the bytes at a given offset within the block.")
  (put-ints! [this offset len values] "Modifies the ints at a given offset within the block. Offset is in Integers.")
  (put-longs! [this offset len values] "Modifies the longs at a given offset within the block. Offset is in Longs.")
  (put-block!
    [this offset src]
    [this offset src src-offset length] "Copies the contents of one block into this block.")
  (copy-over! [this src src-offset] "Replace the contents of this block with another (starting at an offset on the source)."))

(defprotocol BlockManager
  "A mutating object for allocating blocks"
  (allocate-block! [this] "Allocate a new block from the manager's resources.")
  (copy-block! [this block] "Allocates a new block, initialized with a copy of another block.")
  (write-block! [this block] "Writes a block into the managed resources. Flushing is not expected.")
  (get-block [this id] "Returns the block associated with an ID.")
  (rewind! [this] "Revert to the last commit point. Any blocks allocated since the last commit will be invalid.")
  (commit! [this] "Commits all blocks allocated since the last commit. These blocks are now read-only.")
  (copy-to-tx [this block] "Returns a block that is in the current transaction, possibly returning the current block")
  (close [this] "Releases all resources currently in use by this manager."))
