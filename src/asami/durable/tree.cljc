(ns ^{:doc "This namespace provides the basic mechanisms for AVL trees"
      :author "Paula Gearon"}
    asami.durable.tree
  (:require [asami.durable.block.block-api :refer [get-id get-long put-long! put-bytes! get-bytes
                                                   allocate-block! get-block get-block-size
                                                   write-block copy-to-tx]]))

(def ^:const left -1)
(def ^:const right 1)
(def ^:const null "Indicate an invalid node ID" 0)
(def ^:const left-mask 0x3FFFFFFFFFFFFFFF)
(def ^:const balance-mask -0x4000000000000000)  ;; literal value for 0xC000000000000000
(def ^:const balance-bitshift 62)

(def ^:const header-size (* 2 #?(:clj Long/BYTES :cljs BigInt64Array.BYTES_PER_ELEMENT)))

(defprotocol TreeNode
  (get-parent [this] "Returns the parent node. Not the Node ID, but the node object.")
  (get-child-id [this side] "Gets the Node ID of the child on the given side")
  (get-child [this block-manager side] "Gets the Node of the child on the given side")
  (set-child! [this side child] "Sets the ID of the child for the given side")
  (set-balance! [this balance] "Sets the balance of the node")
  (get-balance [this] "Retrieves the balance of the node")
  (get-node-block [this] "Gets this node's underlying r/w block")
  (write [this block-manager] "Persists the node to storage")
  (copy-on-write [this block-manager] "Return a node that is writable in the current transaction"))

(def ^:const left-offset "Offset of the left child, in longs" 0)
(def ^:const right-offset "Offset of the right child, in longs" 1)

(declare get-node ->Node)

(defrecord Node [block parent]
  TreeNode
  (get-parent
    [this]
    parent)
 
  (set-child!
    [this side child]
    (let [child-id (if child (get-id (:block child)) null)]
      (if (= left side)
        (let [balance-bits (bit-and balance-mask (get-long block left-offset))]
          (put-long! block left-offset (bit-or balance-bits child-id)))
        (put-long! block right-offset child-id)))
    this)

  (get-child-id
    [this side]
    (if (= left side)
      (let [v (get-long block left-offset)]
        (bit-and v left-mask))
      (get-long block right-offset)))

  (get-child
    [this block-manager side]
    (let [child-id (get-child-id this side)]
      (if-not (= null child-id)
        (assoc (get-node block-manager child-id this) :side side))))

  (set-balance!
    [this balance]
    (let [left-child (bit-and left-mask (get-long block left-offset))]
      (put-long! block left-offset
                 (bit-or
                  left-child
                  (bit-shift-left balance balance-bitshift)))
      this))

  (get-balance [this]
    (bit-shift-right (get-long block left-offset) balance-bitshift))

  (get-node-block [this] block)

  (write [this block-manager]
    (write-block block-manager block)
    this)

  (copy-on-write [this block-manager]
    (let [new-block (copy-to-tx block-manager block)]
      (if-not (identical? new-block block)
        (->Node new-block (get-parent this))
        this))))

(defrecord Tree [root block-comparator node-comparator block-manager])

(defn get-node
  "Returns a node object for a given ID"
  ([block-manager id]
   (get-node id nil))
  ([block-manager id parent]
   (->Node (get-block block-manager id) parent)))

(defn get-node-id
  [node]
  (get-id (get-node-block node)))

(defn new-node
  "Returns a new node object"
  ([block-manager]
   (new-node block-manager nil nil nil))
  ([block-manager data writer]
   (new-node block-manager data nil writer))
  ([block-manager data parent writer]
   (let [block (allocate-block! block-manager)]
     (when data
       (if writer
         (writer block header-size data)
         (put-bytes! block header-size (count data) data)))
     (->Node block parent))))

(defn init-child!
  [node side child]
  (let [node (set-child! node side child)]
    (set-balance! node (+ side (get-balance node)))))

(defn other [s] (if (= s left) right left))

;; TODO REMOVE
(defn get-data [node] (get-long (get-node-block node) header-size))

(defn rebalance!
  "Rebalance an AVL node. The root of the rebalance is returned unwritten, but all subnodes are written."
  [block-manager node balance]
  (letfn [(rwrite [n] (write n block-manager))
          (rget-child [n s] (get-child n block-manager s))
          (rebalance-ss! [side]
            (let [node-s (rget-child node side)
                  other-side (other side)]
              (set-child! node side (rget-child node-s other-side))
              (set-child! node-s other-side node)
              (rwrite (set-balance! node 0))
              (set-balance! node-s 0))) ;; return node-s
          (rebalance-so! [side]
            (let [other-side (other side)
                  node-s (rget-child node side)
                  node-so (rget-child node-s other-side)]
              (set-child! node side (rget-child node-so other-side))
              (set-child! node-s other-side (rget-child node-so side))
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
              (rwrite node)
              (rwrite node-s)
              (set-balance! node-so 0)))] ;; return node-so
    (let [parent (get-parent node)
          parent-side (:side node)
          side (if (< balance 0) left right)
          new-balance-root (if (= side (get-balance (rget-child node side)))
                             (rebalance-ss! side)
                             (rebalance-so! side))]
      (assoc new-balance-root :parent parent :side parent-side))))

(defn abs
  "Returns the absolute value of the number n"
  [^long n]
  (if (> 0 n) (- n) n))

(defn neighbor-node
  [side block-manager node]
  (if-let [child (get-child node block-manager side)]
    (let [other-side (other side)]
      (loop [n child]
        (if-let [nchild (get-child n block-manager other-side)]
          (recur nchild)
          n)))
    (loop [n node]
      (if-let [parent (get-parent n)]
        (if (= (:side n) side)
          (recur parent)
          parent)))))

(def next-node (partial neighbor-node right))
(def prev-node (partial neighbor-node left))

(defn node-seq
  [block-manager node]
  (when node
    (cons node (lazy-seq (node-seq (next-node block-manager node))))))

(defn find-node
  "Finds a node in the tree using a key.
  returns one of:
  null: an empty tree
  node: the data was found
  vector: the data was not there, and is found between 2 nodes. The leaf node is in the vector.
  The other (unneeded) node is represented by nil."
  [{:keys [root block-comparator block-manager] :as tree} key]
  (letfn [(compare-node [n] (block-comparator key (get-node-block n)))
          (find-node [n]
            #_(println (str "Node: " (get-long (get-node-block n) header-size) "[id:" (get-node-id n) "]"))
            (let [c (compare-node n)]
              #_(println "Compare: " c)
              (if (zero? c)
                n
                (let [side (if (< c 0) left right)]
                  #_(println (if (= side left) "  <<<<" "      >>>>"))
                  (if-let [child (get-child n block-manager side)]
                    (recur child)
                    ;; between this node, and the next/previous
                    (if (= side left)
                      [(prev-node block-manager n) n]
                      [n (next-node block-manager n)]))))))]
    ;(println key " *****************************************************************")
    (and root (find-node root))))

(defn add-to-root
  "Runs back up a branch to update and balance the branch for the transaction.
   node argument is a fully written branch of the tree.
   Returns the root of the tree."
  [block-manager node deeper?]
  (if-let [oparent (get-parent node)]
    (let [side (:side node) ;; these were set during the find operation
          obalance (get-balance oparent)
          parent (-> (copy-on-write oparent block-manager)
                     (set-child! side node))
          [parent next-balance] (if deeper?
                                  (let [b (+ obalance side)]
                                    [(set-balance! parent b) b])
                                  [parent obalance])
          [new-branch ndeeper?] (if (= 2 (abs next-balance))
                                  [(rebalance! block-manager parent next-balance) false]
                                  [parent (and (zero? obalance) (not (zero? next-balance)))])]
      (write new-branch block-manager)
      ;; check if there was change at this level
      (if (= (get-node-id new-branch) (get-node-id oparent))
        (loop [n parent] (if-let [pn (get-parent n)] (recur pn) n)) ;; no change. Short circuit to the root
        (recur block-manager new-branch ndeeper?)))
    node))

(defn add
  "Adds data to the tree"
  [{:keys [root block-manager] :as tree} data & [writer]]
  (if-let [location (find-node tree data)]
    (if (vector? location)
      ;; one of the pair is a leaf node. Attach to the correct side of that node
      (let [[fl sl] location
            [side leaf-node] (if (or (nil? sl) (not= null (get-child-id sl left)))
                               [right fl]
                               [left sl])
            node (write (new-node block-manager data leaf-node writer) block-manager)
            parent-node (copy-on-write leaf-node block-manager)
            pre-balance (get-balance parent-node)
            parent-node (write (init-child! parent-node side node) block-manager)]
        (assoc tree :root (add-to-root block-manager parent-node (zero? pre-balance))))
      tree)
    (assoc tree :root (write (new-node block-manager data writer) block-manager))))

(defn new-block-tree
  "Creates an empty block tree"
  ([block-manager comparator]
   (new-block-tree block-manager comparator nil))
  ([block-manager comparator block-comparator]
   (if-not (get-block block-manager null)
     (throw (ex-info "Unable to initialize tree with null block" {:block-manager block-manager})))
   (let [data-size (- (get-block-size block-manager) header-size)
         block-comparator (or block-comparator
                              (fn [data-a block-b]
                                (comparator data-a (get-bytes block-b header-size data-size))))
         node-comparator (fn [node-a node-b]
                           (block-comparator (get-node-block node-a) (get-node-block node-b)))]
     (->Tree nil block-comparator node-comparator block-manager))))
