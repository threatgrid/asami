(ns ^{:doc "Tuples index with blocks"
      :author "Paula Gearon"}
    asami.durable.tuples
    (:require [asami.durable.common :refer [TupleStorage find-tuple Transaction long-size]]
              [asami.durable.common-utils :as common-utils]
              [asami.durable.tree :as tree]
              [asami.durable.block.block-api :refer [get-long put-long! get-id get-block put-block!
                                                     write-block allocate-block! copy-to-tx]]
              #?(:clj [asami.durable.block.file.block-file :as block-file])))


(def ^:const index-name "Name of the index file" "_stmtidx.bin")

(def ^:const block-name "Name of the block statement file" "_stmt.bin")

(def ^:const tuple-size "The number of values in a tuple" 4)

(def ^:const tuple-size-bytes "The number of bytes in a tuple" (* tuple-size long-size))

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
  (mapv #(get-long node %)
        (range low-tuple-offset (+ low-tuple-offset tuple-size))))

(defn get-high-tuple
  "Returns the high tuple value from the node's range"
  [node]
  (mapv #(get-long node %)
        (range high-tuple-offset (+ high-tuple-offset tuple-size))))

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
  [node tuple]
  (doseq [offset (range 0 tuple-size)]
    (put-long! node (+ low-tuple-offset offset) (nth tuple offset))))

(defn set-high-tuple!
  "Returns the high tuple value from the node's range"
  [node tuple]
  (doseq [offset (range 0 tuple-size)]
    (put-long! node (+ high-tuple-offset offset) (nth tuple offset))))

(defn set-count!
  "Returns the number of tuples in the block"
  [node count*]
  (let [count-code (bit-shift-left count* count-shift)
        new-code (-> node
                     (get-long block-reference-offset)
                     (bit-and reference-mask)
                     (bit-or count-code))]
    (put-long! node block-reference-offset new-code)))

(defn set-block-ref!
  "Gets the ID of the block that contains the tuples"
  [node block-ref]
  (let [new-code (-> node
                     (get-long block-reference-offset)
                     (bit-and count-code-mask)
                     (bit-or block-ref))]
    (put-long! node block-reference-offset new-code)))

(defn tuple-node-compare
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

(defn search-block
  "Returns the tuple offset in a block that matches a given tuple.
  If no tuple matches exactly, then returns a pair of positions to insert between."
  [block len tuple]
  (letfn [(tuple-compare [offset] ;; if the tuple is smaller, then -1, if larger then +1
            (loop [[t & tpl] tuple n 0]
              (if-not t
                0
                (let [bv (get-long block (+ n (* tuple-size offset)))]
                  (cond
                    (< t bv) -1
                    (> t bv) 1
                    :default (recur tpl (inc n)))))))]
    (loop [low 0 high len]
      (if (= (inc low) high)
        (let [r (tuple-compare low)]
          (case r
            0 low
            -1 [(dec low) low]
            1 [low high]))
        (let [mid (int (/ (+ low high) 2))
              c (tuple-compare mid)]
          (case c
            0 mid
            -1 (recur low mid)
            1 (recur mid high)))))))

(defn find-coord
  "Retrieves the coordinate of a tuple.
   A coordinate is a structure of node/pos for the node and offset in the node's block.
   If the tuple is found, then returns a coordinate.
   If the tuple is not found, then return a pair of coordinates that the tuple appears between.
   A nil coordinate in a pair indicates the end of the range of the index."
  [index blocks tuple]
  (let [node (tree/find-node index tuple)]
    (when node
      (if (vector? node)
        (let [[low high] node]
          [(and low {:node low :pos (dec (get-count low))})
           (and high {:node high :pos 0})]))
      (let [block (get-block blocks (get-block-ref node))
            pos (search-block block (get-count node) tuple)]
        (if (vector? pos)
          [{:node node :pos (first pos)} {:node node :pos (second pos)}]
          {:node node :pos pos})))))

(defn tuple-at
  "Retrieves the tuple found at a particular tuple offset"
  [block offset]
  (mapv #(get-long block %) (range offset (+ offset tuple-size))))

(defrecord TupleIndex [index blocks root-id]
  TupleStorage
  (write-tuple! [this tuple]
    (let [insert-coord (find-tuple index blocks)]
      ))

  (delete-tuple! [this tuple]
    (let [delete-coord (find-coord index blocks tuple)]
      (if (or (nil? delete-coord) (vector? delete-coord))
        this
        (let [{:keys [node pos]} delete-coord
              tuple-count (get-count node)
              block-id (get-block-ref node)
              block (get-block blocks block-id)
              ;; modifying the block, which requires copy-on-write
              block (copy-to-tx blocks block)
              new-block-id (get-id block)
              ;; also modifying the tree to refer to this new block
              [new-index node] (tree/modify-node! index node)
              ;; get the offsets in the block to move tuples
              byte-pos (* pos tuple-size-bytes)
              byte-len (* (- tuple-count pos) tuple-size-bytes)
              next-tuple-count (dec tuple-count)]
          ;; move the data in the block down to overwrite the target tuple
          (if (= pos next-tuple-count)
            ;; truncating the block. Change the high tuple. No need to change the block
            (when-not (zero? pos) ;; note: empty blocks will keep the old high tuple
              (set-high-tuple! node (tuple-at block (dec pos))))
            (do
              ;; remove the tuple from the block
              (put-block! block byte-pos block (+ byte-pos tuple-size-bytes) byte-len)
              (when (zero? pos)
                (set-low-tuple! node (tuple-at block 0)))))
          (write-block blocks block)
          ;; update the node's block reference if there is a new block
          (when (not= block-id new-block-id)
            (set-block-ref! node new-block-id))
          ;; decrement the tuple count in the node
          (set-count! node next-tuple-count)
          (tree/write node new-index)
          (assoc this :index new-index :root-id (get-id (:root new-index)))))))

  (find-tuple [this tuple]
    )

  Transaction
  (rewind! [this]
    )

  (commit! [this]
    ))


(defn open-tuples
  [order-name name root-id]
  (let [index (tree/new-block-tree (partial common-utils/create-block-manager name)
                                   (str order-name index-name) tree-node-size tuple-node-compare root-id)
        blocks (common-utils/create-block-manager name (str order-name block-name) (* block-max tuple-size long-size))]
    (->TupleIndex index blocks root-id)))

(defn create-tuple-index
  "Creates a tuple index for a name"
  ([name order-name]
   (create-tuple-index name order-name nil))
  ([name order-name root-id]
   (common-utils/named-storage (partial open-tuples order-name) name root-id)))
