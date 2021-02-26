(ns ^{:doc "This namespace provides the basic mechanisms for AVL trees"
      :author "Paula Gearon"}
    asami.durable.tree
  (:require [asami.durable.block.block-api :refer [Block
                                                   get-id get-byte get-int get-long
                                                   get-bytes get-ints get-longs
                                                   put-byte! put-int! put-long!
                                                   put-bytes! put-ints! put-longs!
                                                   put-block! copy-over!
                                                   allocate-block! get-block get-block-size
                                                   write-block copy-to-tx]]
            [asami.durable.common :refer [Transaction Closeable Forceable close delete! rewind! commit! force! long-size]]
            [asami.durable.cache :refer [lookup hit miss lru-cache-factory]]))

(def ^:const left -1)
(def ^:const right 1)
(def ^:const null "Indicate an invalid node ID" 0)
(def ^:const left-mask 0x3FFFFFFFFFFFFFFF)
(def ^:const balance-mask -0x4000000000000000)  ;; literal value for 0xC000000000000000
(def ^:const balance-bitshift 62)

(def ^:const header-size (* 2 long-size))

(def ^:const header-size-int (bit-shift-right header-size 2))

(def ^:const header-size-long (bit-shift-right header-size 3))

(def ^:const node-cache-size 1024)

(defprotocol TreeNode
  (get-parent [this] "Returns the parent node. Not the Node ID, but the node object.")
  (get-child-id [this side] "Gets the Node ID of the child on the given side")
  (get-child [this tree side] "Gets the Node of the child on the given side")
  (set-child! [this side child] "Sets the ID of the child for the given side")
  (set-balance! [this balance] "Sets the balance of the node")
  (get-balance [this] "Retrieves the balance of the node")
  (write [this tree] "Persists the node to storage")
  (copy-on-write [this tree] "Return a node that is writable in the current transaction"))

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
    [this tree side]
    (let [child-id (get-child-id this side)]
      (if-not (= null child-id)
        (assoc (get-node tree child-id this) :side side))))

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

  (write [this {block-manager :block-manager}]
    (write-block block-manager block)
    this)

  (copy-on-write [this {block-manager :block-manager}]
    (let [new-block (copy-to-tx block-manager block)]
      (if-not (identical? new-block block)
        (assoc (->Node new-block (get-parent this)) :side (:side this))
        this)))

  Block
  (get-id [this]
    (get-id block))

  (get-byte [this offset]
    (get-byte block (+ header-size offset)))

  (get-int [this offset]
    (get-int block (+ header-size-int offset)))

  (get-long [this offset]
    (get-long block (+ header-size-long offset)))

  (get-bytes [this offset len]
    (get-bytes block (+ header-size offset) len))

  (get-ints [this offset len]
    (get-ints block (+ header-size-int offset) len))

  (get-longs [this offset len]
    (get-longs block (+ header-size-long offset) len))

  (put-byte! [this offset value]
    (put-byte! block (+ header-size offset) value))

  (put-int! [this offset value]
    (put-int! block (+ header-size-int offset) value))

  (put-long! [this offset value]
    (put-long! block (+ header-size-long offset) value))

  (put-bytes! [this offset len values]
    (put-bytes! block (+ header-size offset) len values))

  (put-ints! [this offset len values]
    (put-ints! block (+ header-size-int offset) len values))

  (put-longs! [this offset len values]
    (put-longs! block (+ header-size-long offset) len values))

  (put-block! [this offset src]
    (put-block! block (+ header-size offset) src))

  (put-block! [this offset src src-offset length]
    (put-block! block (+ header-size offset) src src-offset length))

  (copy-over! [this src src-offset]
    (throw (ex-info "Unsupported Operation" {}))))

(defn get-node
  "Returns a node object for a given ID"
  ([tree id]
   (get-node tree id nil))
  ([{:keys [block-manager node-cache] :as tree} id parent]
   (if-let [node (lookup @node-cache id)]
     (do
       (swap! node-cache hit id)
       (assoc node :parent parent))
     (let [node (->Node (get-block block-manager id) parent)]
       (swap! node-cache miss id node)
       node))))

(defn new-node
  "Returns a new node object"
  ([tree]
   (new-node tree nil nil nil))
  ([tree data writer]
   (new-node tree data nil writer))
  ([{:keys [block-manager node-cache]} data parent writer]
   (let [block (allocate-block! block-manager)]
     (let [node (->Node block parent)]
       (when data
         (if writer
           (writer node data)
           (put-bytes! node 0 (count data) data)))
       (swap! node-cache miss (get-id block) node)
       node))))

(defn init-child!
  [node side child]
  (let [node (set-child! node side child)]
    (set-balance! node (+ side (get-balance node)))))

(defn other [s] (if (= s left) right left))

(defn rebalance!
  "Rebalance an AVL node. The root of the rebalance is returned unwritten, but all subnodes are written."
  [tree node balance]
  (letfn [(rwrite [n] (write n tree))
          (rget-child [n s] (get-child n tree s))
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
  [side tree node]
  (if-let [child (get-child node tree side)]
    (let [other-side (other side)]
      (loop [n child]
        (if-let [nchild (get-child n tree other-side)]
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
  [tree node]
  (let [node (if (vector? node) (second node) node)
        tree-node-seq (fn tree-node-seq' [n]
                        (when n
                          (cons n
                                (lazy-seq (tree-node-seq' (next-node tree n))))))]
    (when node (tree-node-seq node))))

(defn add-to-root
  "Runs back up a branch to update and balance the branch for the transaction.
   node argument is a fully written branch of the tree.
   Returns the root of the tree."
  [tree node deeper?]
  (if-let [oparent (get-parent node)]
    (let [side (:side node) ;; these were set during the find operation
          obalance (get-balance oparent)
          parent (-> (copy-on-write oparent tree)
                     (set-child! side node))
          [parent next-balance] (if deeper?
                                  (let [b (+ obalance side)]
                                    [(set-balance! parent b) b])
                                  [parent obalance])
          [new-branch ndeeper?] (if (= 2 (abs next-balance))
                                  [(rebalance! tree parent next-balance) false]
                                  [parent (and (zero? obalance) (not (zero? next-balance)))])]
      (write new-branch tree)
      ;; check if there was change at this level
      (if (and (not ndeeper?) (= (get-id new-branch) (get-id oparent)))
        (loop [n parent] (if-let [pn (get-parent n)] (recur pn) n)) ;; no change. Short circuit to the root
        (recur tree new-branch ndeeper?)))
    node))

(defn find-node*
  "Finds a node in the tree using a key.
  returns one of:
  null: an empty tree
  node: the data was found
  vector: the data was not there, and is found between 2 nodes. The leaf node is in the vector.
  The other (unneeded) node is represented by nil."
  [{:keys [root node-comparator] :as tree} key]
  (letfn [(compare-node [n] (node-comparator key n))
          (find-node [n]
            (let [c (compare-node n)]
              (if (zero? c)
                n
                (let [side (if (< c 0) left right)]
                  (if-let [child (get-child n tree side)]
                    (recur child)
                    ;; between this node, and the next/previous
                    (if (= side left)
                      [(prev-node tree n) n]
                      [n (next-node tree n)]))))))]
    (and root (find-node root))))

(defprotocol Tree
  (find-node [this key] "Finds a node in the tree using a key.")
  (add [this data writer] [this data writer location] "Adds data to the tree")
  (at [this new-root] "Returns a tree for a given transaction root")
  (modify-node! [this node]
    "Makes a node available to modify in the current transaction.
     Returns the new tree and a node that can be modified (possible the same node)"))

(defrecord ReadOnlyTree [root node-comparator block-manager node-cache]
  Tree
  (find-node [this key] (find-node* this key))
  (add [this data writer] (throw (ex-info "Read-only trees cannot be modified" {:add data})))
  (at [this new-root-id] (assoc this :root (get-node this new-root-id nil))))


(defrecord TxTree [root rewind-root node-comparator block-manager node-cache]
  Tree
  (find-node [this key] (find-node* this key))

  (add [this data writer]
    (add this data writer (find-node this data)))

  (add [this data writer location]
    (if location
      (if (vector? location)
        ;; one of the pair is a leaf node. Attach to the correct side of that node
        (let [[fl sl] location
              [side leaf-node] (if (or (nil? sl) (not= null (get-child-id sl left)))
                                 [right fl]
                                 [left sl])
              node (write (new-node this data leaf-node writer) this)
              parent-node (copy-on-write leaf-node this)
              pre-balance (get-balance parent-node)
              parent-node (write (init-child! parent-node side node) this)]
          (assoc this :root (add-to-root this parent-node (zero? pre-balance))))
        this)
      (assoc this :root (write (new-node this data writer) this))))

  (at [this new-root-id]
    (let [new-root (get-node this new-root-id nil)]
      (->ReadOnlyTree new-root node-comparator block-manager node-cache)))

  (modify-node! [this node]
    ;; copy this node. It will be returned without write being called for it.
    (let [new-node (copy-on-write node this)]
      (if (identical? node new-node)
        [this node]
        ;; iterate towards the root, copying into the transaction
        ;; modified-node remembers the node to be returned
        (loop [nd new-node modified-node nil]
          (let [parent (get-parent nd)]
            (if parent
              ;; copy the parent, setting it to refer to the current node
              (let [new-parent (-> (copy-on-write parent this)
                                   (set-child! (:side nd) nd)
                                   (write this))
                    ;; modified-node has not been set if this is the first time through
                    ;; update the current node with the new parent, and store for returning
                    modified-node (or modified-node (assoc nd :parent new-parent))]
                (if (identical? parent new-parent)
                  ;; parent did not change, so it was already in the new transaction. Short circuit
                  [this modified-node]
                  ;; continue toward the root, remembering the return node
                  (recur new-parent modified-node)))
              ;; no parent, so nd is the root
              ;; if modified-node is not yet set, then new-node was the root
              [(assoc this :root nd) (or modified-node new-node)]))))))

  Transaction
  (rewind! [this]
    (rewind! block-manager)
    (assoc this :root rewind-root))

  (commit! [this]
    (commit! block-manager)
    (assoc this :rewind-root root))
  
  Forceable
  (force! [this]
    (force! block-manager))

  Closeable
  (close [this]
    (close block-manager))

  (delete! [this]
    (delete! block-manager)))


(defn new-block-tree
  "Creates an empty block tree"
  ([block-manager-factory store-name data-size node-comparator]
   (new-block-tree block-manager-factory store-name data-size node-comparator nil))
  ([block-manager-factory store-name data-size node-comparator root-id]
   (let [block-manager (block-manager-factory store-name (+ header-size data-size))]
     (if-not (get-block block-manager null)
       (throw (ex-info "Unable to initialize tree with null block" {:block-manager block-manager})))
     (let [data-size (- (get-block-size block-manager) header-size)
           root (if (and root-id (not= null root-id))
                  (->Node (get-block block-manager root-id) nil))]
       (->TxTree root root node-comparator block-manager
               (atom (lru-cache-factory {} :threshold node-cache-size)))))))
