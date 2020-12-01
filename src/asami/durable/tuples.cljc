(ns ^{:doc "Tuples index with blocks"
      :author "Paula Gearon"}
    asami.durable.tuples
  (require [asami.durable.common :as common :refer [TupleIndex find-tuple Transaction]]
           [asami.durable.tree :as tree]
           [asami.durable.block.block-api :refer [get-long put-long! get-id]]
           #?(:clj [asami.durable.block.file.block-file :as block-file])))


(def ^:const index-name "Name of the index file" "_stmtidx.bin")

(def ^:const block-name "Name of the block statement file" "_stmt.bin")

(def ^:const tuple-size "The number of values in a tuple" 4)

;; All offsets are measured in longs.
;; Byte offsets can be calculated by multiplying these by long-size

(def ^:const low-tuple-offset 0)

(def ^:const high-tuple-offset 4)

(def ^:const block-reference-offset 8) ;; The block reference also contains the count

(def ^:const tree-node-size "Number of bytes used in the index nodes" (* (inc block-reference-offset) long-size))

;; range is 0-511
(def ^:const block-max "Maximum number of tuples in a block" 512)

;; 9 bits required to measure block-size
(def ^:const reference-mask 0x007FFFFFFFFFFFFF)

(def ^:const count-code-mask -0x80000000000000) ;; 0xFF80000000000000

(def ^:const count-shift "The shift to get the count bits" 55)

(def ^:const count-mask "The mask to apply for the count bits" 0x1FF)

(defn get-low-tuple
  "Returns the low tuple value from the node's range"
  [node]
  [(get-long node low-tuple-offset)
   (get-long node (+ 1 low-tuple-offset))
   (get-long node (+ 2 low-tuple-offset))
   (get-long node (+ 3 low-tuple-offset))])

(defn get-high-tuple
  "Returns the high tuple value from the node's range"
  [node]
  [(get-long node high-tuple-offset)
   (get-long node (+ 1 high-tuple-offset))
   (get-long node (+ 2 high-tuple-offset))
   (get-long node (+ 3 high-tuple-offset))])

(defn get-count
  "Returns the number of tuples in the block"
  [node]
  (-> node
      (get-long block-reference-offset)
      (bit-shift-right count-shift)
      (bit-and count-mask)))

(defn get-block-ref
  "Gets the ID of the block that contains the tuples"
  [node]
  (-> node
      (get-long block-reference-offset)
      (bit-and reference-mask)))

(defn set-low-tuple!
  "Returns the low tuple value from the node's range"
  [node [tuple-a tuple-b tuple-c tuple-d]]
  [(set-long! node low-tuple-offset tuple-a)
   (set-long! node (+ 1 low-tuple-offset) tuple-b)
   (set-long! node (+ 2 low-tuple-offset) tuple-c)
   (set-long! node (+ 3 low-tuple-offset) tuple-d)])

(defn set-high-tuple!
  "Returns the high tuple value from the node's range"
  [node [tuple-a tuple-b tuple-c tuple-d]]
  [(set-long! node high-tuple-offset tuple-a)
   (set-long! node (+ 1 high-tuple-offset) tuple-b)
   (set-long! node (+ 2 high-tuple-offset) tuple-c)
   (set-long! node (+ 3 high-tuple-offset) tuple-d)])

(defn set-count!
  "Returns the number of tuples in the block"
  [node count*]
  (let [count-code (bit-shift-left count* count-shift)
        new-code (-> node
                     (get-long block-reference-offset)
                     (bit-and reference-mask)
                     (bit-or count-code))]
    (set-long! node block-reference-offset new-code)))

(defn set-block-ref!
  "Gets the ID of the block that contains the tuples"
  [node block-ref]
  (let [new-code (-> node
                     (get-long block-reference-offset)
                     (bit-and count-code-mask)
                     (bit-or block-ref))]
    (set-long! node block-reference-offset new-code)))

(def tuple-node-compare
  "Compare the contents of a tuple to the range of a node"
  [tuple node]
  (loop [[t & tpl] tuple offset 0]
    (if-not t
      0
      (let [low (get-long node (+ low-tuple-offset offset))]
        (if (< t low)
          -1
          (let [high (get-long node (+ high-tuple-offset offset))]
            (if (> t high)
              1
              (recur tpl (inc offset)))))))))

(defrecord TupleIndex [index blocks root-id]
  TupleStorage
  (write-tuple! [this tuple]
    )

  (delete-tuple! [this tuple]
    )

  (find-tuple [this tuple]
    )

  Transaction
  (rewind! [this]
    )

  (commit! [this]
    ))


(defn open-tuples
  [order-name name root-id]
  (let [index (tree/new-block-tree (partial common/create-block-manager name)
                                   (str order-name index-name) tree-node-size tuple-node-compare root-id)
        blocks (common/create-block-manager name (str order-name block-name) (* max-block tuple-size long-size))]
    (->TupleIndex index blocks root-id)))

(defn create-tuple-index
  "Creates a tuple index for a name"
  ([name order-name]
   (create-tuple-index name  order-namenil))
  ([name  order-nameroot-id]
   (common/named-storage (partial open-tuples order-name) name root-id)))
