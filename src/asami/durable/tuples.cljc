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
  "Sets the low tuple value of the node's range"
  [node tuple]
  (doseq [offset (range 0 tuple-size)]
    (put-long! node (+ low-tuple-offset offset) (nth tuple offset))))

(defn set-high-tuple!
  "Sets the high tuple value of the node's range"
  [node tuple]
  (doseq [offset (range 0 tuple-size)]
    (put-long! node (+ high-tuple-offset offset) (nth tuple offset))))

(defn set-count!
  "Sets the number of tuples in the block"
  [node count*]
  (let [count-code (bit-shift-left count* count-shift)
        new-code (-> node
                     (get-long block-reference-offset)
                     (bit-and reference-mask)
                     (bit-or count-code))]
    (put-long! node block-reference-offset new-code)))

(defn set-block-ref!
  "Sets the ID of the block that contains the tuples"
  [node block-ref]
  (let [new-code (-> node
                     (get-long block-reference-offset)
                     (bit-and count-code-mask)
                     (bit-or block-ref))]
    (put-long! node block-reference-offset new-code)))

(defn tuple-node-compare
  "Compare the contents of a tuple to the range of a node"
  [tuple node]
  (loop [[t & tpl] tuple offset 0 low-test? true high-test? true]
    (if-not t
      ;; always compare a missing element as less than an existing one
      (cond
        (= offset tuple-size) 0 ;; no missing elements
        low-test? -1 ;; still testing against low, so smaller than this node
        high-test? 0 ;; still testing against high, so within this node
        :default -1) ;; sanity test: not possible
      (let [[return low-test?] (if low-test?
                                 (let [low (get-long node (+ low-tuple-offset offset))]
                                   (cond
                                     (< t low) [-1 nil]
                                     (> t low) [nil false]
                                     :default [nil true]))
                                 [nil false])]
        (or
         return
         (let [[return high-test?] (if high-test?
                                     (let [high (get-long node (+ high-tuple-offset offset))]
                                       (cond
                                         (> t high) [1 nil]
                                         (< t high) [nil false]
                                         :default [nil true]))
                                     [nil false])]
           (or
            return
            (if (not (or low-test? high-test?)) 0)
            (recur tpl (inc offset) low-test? high-test?))))))))

(defn search-block
  "Returns the tuple offset in a block that matches a given tuple.
  If no tuple matches exactly, then returns a pair of positions to insert between."
  [block len tuple]
  (letfn [(tuple-compare [offset partials?] ;; if the tuple is smaller, then -1, if larger then +1
            (loop [[t & tpl] tuple n 0]
              (if-not t
                ;; end of the input tuple. If it's an entire tuple then it's equal,
                ;; otherwise it's a partial tuple and considered less than
                (if (or partials? (= n tuple-size)) 0 -1)
                (let [bv (get-long block (+ n (* tuple-size offset)))]
                  (cond
                    (< t bv) -1
                    (> t bv) 1
                    :default (recur tpl (inc n)))))))]
    (loop [low 0 high len]
      (if (>= (inc low) high) ;; the >= catches an empty block, though these should not be searched
        ;; finished the search. Return the offset when found or a pair when not found
        (case (tuple-compare low false)
          0 low
          -1 (if (zero? (tuple-compare low true))
               low
               [(dec low) low])
          1 (if (zero? (tuple-compare high true))
              high
              [low high]))
        (let [mid (int (/ (+ low high) 2))
              c (tuple-compare mid false)]
          (case c
            0 mid
            -1 (recur low mid)
            1 (recur mid high)))))))

(defn partial-equal
  "Compares tuples. If one is only partial, then they are allowed to compare equal"
  [[t1 & r1] [t2 & r2]]
  (or (nil? t1) (nil? t2)
      (and (= t1 t2) (recur r1 r2))))

(defn find-coord
  "Retrieves the coordinate of a tuple.
   A coordinate is a structure of node/pos for the node and offset in the node's block.
   If the tuple is found, then returns a coordinate.
   If the tuple is not found, then return a pair of coordinates that the tuple appears between.
   A nil coordinate in a pair indicates the end of the range of the index."
  [index blocks tuple]
  (let [node (tree/find-node index tuple)]
    ;; empty trees return nothing
    (when node
      (if (vector? node)
        ;; 2 nodes means that the point is between the top of the lower node,
        ;; and the bottom of the higher node
        (let [[low high] node]
          (if (and high (partial-equal tuple (get-low-tuple high)))
            {:node high :pos 0}
            [(and low {:node low :pos (dec (get-count low))})
             (and high {:node high :pos 0})]))
        ;; single node, so look inside for the point
        (let [block (get-block blocks (get-block-ref node))
              block-len (get-count node)]
          ;; empty nodes have the position of their last single tuple
          ;; if the tuple matched then give a point between the empty node and the next
          (if (zero? block-len)
            [{:node node :pos 0} {:node (tree/next-node index node) :pos 0}]
            ;; search within the node for the coordinate
            (let [pos (search-block block block-len tuple)]
              (if (vector? pos)
                [{:node node :pos (first pos)} {:node node :pos (second pos)}]
                {:node node :pos pos}))))))))

(defn tuple-at
  "Retrieves the tuple found at a particular tuple offset"
  [block offset]
  (let [long-offset (* offset tuple-size)]
    (mapv #(get-long block %) (range long-offset (+ long-offset tuple-size)))))

(defn set-tuple-at!
  "Retrieves the tuple found at a particular tuple offset"
  [block offset tuple]
  (let [long-offset (* offset tuple-size)]
    (doseq [n (range tuple-size)]
      (put-long! block (+ n long-offset) (nth tuple n)))))

(defn tuple-seq
  "Create a lazy sequence of tuples.
  This iterates through the block associated with the given node, until reaching the size of the block.
  Then moves to the next node, and starts iterating through the block associated with it.
  Continues as long as the provided tuple matches the tuple in the block.
  index: the tree index with the nodes
  blocks: the block manager for tuples blocks
  tuple: the tuple being searched for. Every returned tuple with match this one by prefix
  offset: the starting point of iteration"
  [index blocks tuple node offset]
  (let [nodes (tree/node-seq index node)
        block-for (fn [n] (and n (get-block blocks (get-block-ref n))))
        nested-seq (fn nested-seq' [[n & ns :as all-ns] blk offs]
                     (when n
                       (let [t (tuple-at blk offs)]
                         (when (partial-equal tuple t)
                           (let [nxto (inc offs)
                                 [nnodes nblock noffset] (if (= nxto (get-count n))
                                                           (loop [[fns & rns :as alln] ns]
                                                             (if (and fns (zero? (get-count fns)))
                                                               (recur rns)
                                                               [alln (block-for fns) 0]))
                                                           [all-ns blk nxto])]
                             (cons t (lazy-seq (nested-seq' nnodes nblock noffset))))))))]
    (nested-seq nodes (block-for node) offset)))

(defn init-node-writer
  [block-id node tuple]
  (set-low-tuple! node tuple)
  (set-high-tuple! node tuple)
  (set-count! node 1)
  (set-block-ref! node block-id))

(defrecord TupleIndex [index blocks root-id]
  TupleStorage
  (write-tuple! [this tuple]
    (let [insert-coord (find-coord index blocks tuple)]
      (cond
        (nil? insert-coord) (let [block (allocate-block! blocks)
                                  idx (tree/add index tuple (partial init-node-writer (get-id block)) nil)]
                              (set-tuple-at! block 0 tuple)
                              (assoc this :index idx :root-id (:root idx)))
        (vector? insert-coord)  ;; insert into existing block
        (let [{lnode :node lpos :pos} {hnode :node hpos :pos}]
          (if (= lnode hnode)  ;; modifying this node
            (let [[new-index node] (tree/modify-node! index lnode)
                  old-block-id (get-block-ref node) 
                  block (->> old-block-id
                             (get-block blocks)
                             (copy-to-tx blocks))
                  block-id (get-id block)
                  tuple-count (get-count node)
                  byte-pos (* hpos tuple-size-bytes)
                  byte-len (* (- tuple-count hpos) tuple-size-bytes)]
              (if (= block-max (get-count lnode))
                ;; TODO: full block. Must split
                (do
                  (put-block! block (+ byte-pos tuple-size-bytes) block byte-pos byte-len)
                  (set-tuple-at! block hpos tuple)
                  (write-block blocks block)
                  (when (not= old-block-id block-id)
                    (set-block-ref! node block-id))
                  (set-count! node (inc tuple-count))
                  (tree/write node new-index)
                  (assoc this :index new-index :root-id (get-id (:root new-index))))))
            ;; else between nodes. TODO: Figure out where to add... recurse to above
            ))

        :default this)))  ;; do nothing

  (delete-tuple! [this tuple]
    (let [delete-coord (find-coord index blocks tuple)]
      (if (or (nil? delete-coord) (vector? delete-coord))
        this
        (let [{:keys [node pos]} delete-coord
              tuple-count (get-count node)
              block-id (get-block-ref node)
              ;; modifying the block, which requires copy-on-write
              block (->> block-id (get-block blocks) (copy-to-tx blocks))
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

  (find-tuple [this tuple] ;; full length tuples do an existence search. Otherwise, build a seq
    (let [pos (find-coord index blocks tuple)]
      (if (map? pos)
        ;; short circuit for existence test
        (if (= tuple-size (count tuple))
          (when (map? pos) [tuple])
          (let [{:keys [node offset]} pos]
            (tuple-seq index blocks tuple node offset)))
        [])))

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
