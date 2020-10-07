(ns ^{:doc "This namespace provides the basic mechanisms for AVL trees"
      :author "Paula Gearon"}
    asami.durable.tree
  (:require [asami.durable.block.block-api]))

(def ^:dynamic *block-manager* nil)

(def ^:const left -1)
(def ^:const right 1)
(def ^:const null "Indicate an invalid node ID" 0)
(def ^:const left-mask 0x3FFFFFFFFFFFFFFF)
(def ^:const balance-mask -0x4000000000000000)  ;; literal value for 0xC000000000000000
(def ^:const balance-bitshift 62)

(defprotocol TreeNode
  (set-child! [this side child] "Sets the ID of the child for the given side")
  (get-child [this side] "Gets the ID of the child on the given side")
  (set-balance! [this balance] "Sets the balance of the node")
  (get-balance [this] "Retrieves the balance of the node")
  (get-block [this] "Gets this node's underlying r/w block"))

(def ^:const left-offset "Offset of the left child, in longs" 0)
(def ^:const right-offset "Offset of the right child, in longs" 1)

(declare get-node)

(defrecord Node [block]
  (set-child!
    [this side child]
    (let [child-id (getid (:block child))]
      (if (= left side)
        (let [balance-bits (bit-and balance-mask (get-long block left-offset))]
          (put-long! block left-offset (bit-or balance-bits child-id)))
        (put-long! block right-offset child-id)))
    this)

  (get-child
    [this side]
    (let [child-id (if (= left side)
                     (let [v (get-long block left-offset)]
                       (bit-and v left-mask))
                     (get-long block right-offset))]
      (if-not (= null child-id)
        (get-node child-id))))

  (set-balance!
    [this balance]
    (let [left-child (bit-and left-mask (get-long block left-offset))]
      (set-long! block left-offset
                 (bit-or
                  left-child
                  (bit-shift-left balance balance-bitshift)))))

  (get-balance [this]
    (bit-shift-right balance-bitshift (get-long block left-offset)))

  (get-block [this] block))

(defn get-node
  "Returns a node object for a given ID"
  [id]
  (->Node (get-block *block-manager* id)))

(defrecord Tree [root comparator])

(defn init-child!
  [node side child]
  (let [node (set-child! node side child)]
    (set-balance! node (+ side (get-balance node)))))

(defn rebalance
  "Rebalance an AVL node"
  [node balance])

(defn abs
  "Returns the absolute value of the number n"
  [^long n]
  (if (> 0 n) (- n) n))

(defn add-node
  "Adds a node to the tree, returning the new tree"
  [{:keys [root comparator] :as tree} node]
  (letfn [(insert-node! [tree-node new-node]
            (let [side (if (neg? (comparator tree-node new-node)) left right)]
              (if-let [child (get-child side)]
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
                    (rebalance tree-node next-balance)
                    tree-node))
                (init-child! tree-node side node))))]
    (if root
      (let [new-root (insert-node! comparator root node)]
        (if (= new-root root)
          tree
          (assoc tree :root new-root)))
      (assoc tree :root node))))

