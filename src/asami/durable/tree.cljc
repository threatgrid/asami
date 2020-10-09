(ns ^{:doc "This namespace provides the basic mechanisms for AVL trees"
      :author "Paula Gearon"}
    asami.durable.tree
  (:require [asami.durable.block.block-api]))

(def ^:const left -1)
(def ^:const right 1)
(def ^:const null "Indicate an invalid node ID" 0)
(def ^:const left-mask 0x3FFFFFFFFFFFFFFF)
(def ^:const balance-mask -0x4000000000000000)  ;; literal value for 0xC000000000000000
(def ^:const balance-bitshift 62)

(def ^:const header-size (* 2 #?(:clj Long/BYTES :cljs BigInt64Array.BYTES_PER_ELEMENT)))

(defprotocol TreeNode
  (get-parent [this] "Returns the parent node. Not the Node ID, but the node object.")
  (set-child! [this side child] "Sets the ID of the child for the given side")
  (get-child [this block-manager side] "Gets the ID of the child on the given side")
  (set-balance! [this balance] "Sets the balance of the node")
  (get-balance [this] "Retrieves the balance of the node")
  (get-block [this] "Gets this node's underlying r/w block")
  (write [this] "Persists the node to storage")
  (copy-on-write [this block-manager] "Return a node that is writable in the current transaction"))

(def ^:const left-offset "Offset of the left child, in longs" 0)
(def ^:const right-offset "Offset of the right child, in longs" 1)

(declare get-node ->Node)

(defrecord Node [block parent]
  (set-child!
    [this side child]
    (let [child-id (getid (:block child))]
      (if (= left side)
        (let [balance-bits (bit-and balance-mask (get-long block left-offset))]
          (put-long! block left-offset (bit-or balance-bits child-id)))
        (put-long! block right-offset child-id)))
    this)

  (get-child
    [this block-manager side]
    (let [child-id (if (= left side)
                     (let [v (get-long block left-offset)]
                       (bit-and v left-mask))
                     (get-long block right-offset))]
      (if-not (= null child-id)
        (get-node block-manager child-id this))))

  (set-balance!
    [this balance]
    (let [left-child (bit-and left-mask (get-long block left-offset))]
      (set-long! block left-offset
                 (bit-or
                  left-child
                  (bit-shift-left balance balance-bitshift)))
      this))

  (get-balance [this]
    (bit-shift-right balance-bitshift (get-long block left-offset)))

  (get-block [this] block)

  (write [this block-manager]
    (write-block block-manager block)
    this)

  (copy-on-write [this block-manager]
    (let [new-block (copy-to-tx block-manager block)]
      (if-not (identical? new-block block)
        (->Node new-block this)
        this))))

(defrecord Tree [root comparator node-comparator block-manager])

(defn get-node
  "Returns a node object for a given ID"
  ([block-manager id]
   (get-node id nil))
  ([block-manager id parent]
   (->Node (get-block block-manager id) parent)))

(defn new-node
  "Returns a new node object"
  ([block-manager]
   (new-node block-manager nil nil))
  ([block-manager data]
   (new-node block-manager data nil))
  ([block-manager data parent]
   (let [block (allocate-block! block-manager)]
     (when data
       (put-bytes! block header-size (count data) data))
     (->Node block parent))))

(defn init-child!
  [node side child]
  (let [node (set-child! node side child)]
    (set-balance! node (+ side (get-balance node)))))

(defn other [s] (if (= s left) right left))

(defn rebalance!
  "Rebalance an AVL node. The root of the rebalance is returned unwritten, but all subnodes are written."
  [block-manager node balance]
  (letfn [(write [n] (write n block-manager))
          (get-child [n s] (get-child n block-manager s))
          (rebalance-ss! [side]
            (let [node-s (get-child node side)
                  other-side (other side)]
              (set-child! node side (get-child node-s other-side))
              (set-child! node-s other-side node)
              (write (set-balance! node 0))
              (set-balance! node-s 0)))  ;; return node-s
          (rebalance-so! [side]
            (let [other-side (other side)
                  node-s (get-child node side)
                  node-so (get-child node-s other-side)]
              (set-child! node side (get-child node-so other-side))
              (set-child! node-s other-side (get-child node-so side))
              (set-child! node-so other-side node)
              (set-child! node-so side node-s)
              (condp = (get-balance node-so)
                other-side (do
                             (set-balance! node 0)
                             (set-balance! node-s side))
                side (do
                       (set-balance! node other-side)
                       (set-balance! node-s 0))
                (do
                  (set-balance! node 0)
                  (set-balance! node-s 0)))
              (write node)
              (write node-s)
              (set-balance! node-so 0)))] ;; return node-so
    (let [side (if (< balance 0) left right)]
      (if (= side (get-balance (get-child node block-manager side)))
        (rebalance-ss! side)
        (rebalance-so! side)))))

(defn abs
  "Returns the absolute value of the number n"
  [^long n]
  (if (> 0 n) (- n) n))

(defn add-node
  "Adds a node to the tree, returning the new tree"
  [{:keys [root node-comparator block-manager] :as tree} node]
  (letfn [(write [n] (write n block-manager))
          (insert-node! [tree-node new-node]
            (let [side (if (neg? (node-comparator tree-node new-node)) left right)
                  tree-node (copy-on-write tree-node block-manager)]
              (if-let [child (get-child tree-node block-manager side)]
                (let [child-balance (balance child)
                      inserted-branch (insert-node! comparator child new-node)
                      tree-node (set-child! tree-node side inserted-branch)
                      next-balance (get-balance tree-node)
                      ;; did the child balance change from 0? Then it's deeper
                      [tree-node next-balance] (if (and (zero? child-balance)
                                                        (not= 0 (get-balance inserted-branch)))
                                                 (let [b (+ next-balance side)]
                                                   [(set-balance! tree-node b) b])
                                                 [tree-node next-balance])]
                  (if (= 2 (abs next-balance))
                    (rebalance! block-manager tree-node next-balance) ;; rebalanceed nodes will be written
                    tree-node))
                (init-child! tree-node side node))))]
    (write node)
    (if root
      (let [new-root (write (insert-node! comparator root node))]
        (if (= new-root root)
          tree
          (assoc tree :root new-root)))
      (assoc tree :root node))))

(defn add
  "Adds data to the tree, where the data is a byte array"
  [{:keys [root comparator block-manager :as tree]} b]
  (add-node (new-node block-manager b)))

(defn previous-node
  "Find the next node in the tree"
  [n]
  ;; TODO
  )

(defn next-node
  "Find the next node in the tree"
  [n]
  ;; TODO
  )

(defn find
  "Finds a node in the tree using a key"
  [{:keys [root comparator block-manager :as tree]} key]
  (letfn [(compare-node [n] (comparator key (get-bytes (get-block n) header-size data-size)))
          (find [n]
            (let [c (compare-node n)]
              (if (zero? c)
                n
                (let [side (if (< c 0) left right)]
                  (if-let [child (get-child n block-manager side)]
                    (find child)
                    ;; between this node, and the next/previous
                    (if (= side left)
                      [(previous-node n) n]
                      [n (next-node n)]))))))]
    (find root)))

(defn new-block-tree
  "Creates an empty block tree"
  [comparator block-manager]
  (let [data-size (- (get-block-size block-manager) header-size)
        node-comparator (fn [a b]
                          (comparator (get-bytes (get-block a) header-size data-size)
                                      (get-bytes (get-block b) header-size data-size)))]
    (->Tree nil comparator node-comparator block-manager)))
