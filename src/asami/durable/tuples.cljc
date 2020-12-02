(ns ^{:doc "Tuples index with blocks"
      :author "Paula Gearon"}
    asami.durable.tuples
    (:require [asami.durable.common :refer [TupleStorage find-tuple Transaction long-size]]
              [asami.durable.common-utils :as common-utils]
              [asami.durable.tree :as tree]
              [asami.durable.block.block-api :refer [get-long put-long! get-id get-block
                                                     write-block allocate-block! copy-to-tx]]
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
        (if (zero? (tuple-compare low))
          low
          [low high])
        (let [mid (int (/ (+ low high) 2))
              c (tuple-compare mid)]
          (cond
            (zero? c) mid
            (> 0 c) (recur low mid)
            (< 0 c) (recur mid high)))))))

(defn find-coord
  [index blocks tuple]
  (let [node (tree/find-node index tuple)]
    (when node
      (if (vector? node)
        (let [[low high] node]
          [{:node low :pos (dec (get-count low))} {:node high :pos 0}]))
      (let [block (get-block blocks (get-block-ref node))
            pos (search-block block (get-count node) tuple)]
        (if (vector? pos)
          [{:node node :pos (first pos)} {:node node :pos (second pos)}]
          {:node node :pos pos})))))

(defrecord TupleIndex [index blocks root-id]
  TupleStorage
  (write-tuple! [this tuple]
    (let [insert-coord (find-tuple index blocks)]
      ))

  (delete-tuple! [this tuple]
    (let [delete-coord (find-coord index blocks tuple)]
      (if (or (nil? delete-coord) (vector? delete-coord))
        this
        ;; TODO: remove
        )))

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
