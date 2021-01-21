(ns ^{:doc "Tests for the tuples store"
      :author "Paula Gearon"}
    asami.durable.tuples-test
  (:require [clojure.test :refer [deftest is]]
            [asami.durable.test-utils :refer [new-block]]
            [asami.durable.common :refer [Transaction long-size delete-tuple! find-tuple
                                          write-tuple! write-new-tx-tuple!]]
            [asami.durable.tree :as tree]
            [asami.durable.tuples :refer [search-block tuple-size tree-node-size block-max
                                          get-low-tuple get-high-tuple
                                          set-low-tuple! set-high-tuple!
                                          get-count get-block-ref
                                          set-count! set-block-ref!
                                          tuple-at set-tuple-at!
                                          tree-node-size tuple-node-compare
                                          tuple-seq
                                          ->TupleIndex find-coord]]
            [asami.durable.block.block-api :refer [BlockManager allocate-block! get-id get-long put-long!
                                                   get-block copy-block! copy-over!]]))

(defn add-tuples
  [block tuples]
  (reduce-kv
   (fn [b n tuple]
     (let [tuple-offset (* n tuple-size)]
       (doseq [offset (range tuple-size)]
         (put-long! block (+ tuple-offset offset) (nth tuple offset)))
       block))
   block
   tuples))

(def tuples-block-size (* block-max tuple-size long-size))

(deftest tuple-block-search
  (let [block (new-block tuples-block-size)
        tuples [[1 2 3 1]
                [1 2 5 1]
                [1 5 3 1]
                [1 6 2 1]
                [10 2 3 1]
                [10 2 4 1]
                [10 5 7 1]
                [10 6 2 1]
                [15 2 4 1]
                [16 2 3 1]
                [16 6 2 1]]
        len (count tuples)
        block (add-tuples block tuples)]
    (is (= 3 (search-block block len [1 6 2])))
    (is (= 3 (search-block block len [1 6 2 1])))
    (is (= 4 (search-block block len [10 2 3])))
    (is (= 4 (search-block block len [10 2 3 1])))
    (is (= 0 (search-block block len [1 2 3])))
    (is (= 0 (search-block block len [1 2 3 1])))
    (is (= 10 (search-block block len [16 6 2])))
    (is (= 10 (search-block block len [16 6 2 1])))

    (is (= [0 1] (search-block block len [1 2 4])))
    (is (= [1 2] (search-block block len [1 3 40])))
    (is (= [3 4] (search-block block len [4 1 1])))

    ;; testing edge case. This is not called
    (is (= [-1 0] (search-block block len [1 1 1])))
    (is (= [10 11] (search-block block len [16 7 0])))))

(def tree-block-size (+ tree-node-size tree/header-size))

(defrecord FauxManager [block-list block-size commit-point]
  BlockManager
  (allocate-block! [this]
    (let [b (new-block block-size (count @block-list))]
      (swap! block-list conj b)
      b))
  (copy-block! [this block] (copy-over! (allocate-block! this) block 0))
  (write-block [this block] this)
  (get-block [this id] (nth @block-list id))
  (get-block-size [this] tuples-block-size)
  (copy-to-tx [this block] (if (<= (get-id block) @commit-point)
                             (copy-block! this block)
                             block))
  Transaction
  (rewind! [this] (swap! block-list subvec 0 commit-point))
  (commit! [this] (swap! commit-point constantly (count @block-list)) this))

(defn faux-manager [s] (->FauxManager (atom []) s (atom 0)))

(defn print-tuple-block
  [block]
  (println (str "---- block " (get-id block) " ----"))
  (doseq [t (->> (range block-max)
                 (map #(tuple-at block %))
                 (take-while #(not= [0 0 0 0] %)))]
    (println t))
  (println "-----------------"))

(defn print-node
  [node]
  (println (str "---- node " (get-id node) " ----"))
  (println "low:" (get-low-tuple node))
  (println "high:" (get-high-tuple node))
  (println "count:" (get-count node))
  (println "tuples block:" (get-block-ref node))
  (let [l (tree/get-child-id node tree/left)
        r (tree/get-child-id node tree/right)]
    (when (not= 0 l) (print "left: " l " "))
    (when (not= 0 r) (print "right: " r))
    (when (not= 0 (+ l r)) (println)))
  (println "----------------"))

(defn create-test-tree
  []
  (let [node-manager (faux-manager tree-block-size)
        null (allocate-block! node-manager)
        _ (assert (zero? (get-id null)))
        root (tree/->Node (allocate-block! node-manager) nil)
        lchild (assoc (tree/->Node (allocate-block! node-manager) root) :side tree/left)
        rchild (assoc (tree/->Node (allocate-block! node-manager) root) :side tree/right)
        rlchild (assoc (tree/->Node (allocate-block! node-manager) rchild) :side tree/left)
        blocks (faux-manager tuples-block-size)
        block1 (allocate-block! blocks)
        block2 (allocate-block! blocks)
        block3 (allocate-block! blocks)
        block4 (allocate-block! blocks)
        index (tree/new-block-tree (constantly node-manager)
                                   ""
                                   tree-node-size
                                   tuple-node-compare
                                   (get-id root))
        tuples (->TupleIndex index blocks (get-id root))]
    (set-low-tuple! lchild [2 4 6 1])
    (set-high-tuple! lchild [2 8 8 1])
    (set-count! lchild 2)
    (set-block-ref! lchild (get-id block1))
    (set-tuple-at! block1 0 [2 4 6 1])
    (set-tuple-at! block1 1 [2 8 8 1])
    
    (set-low-tuple! root [2 8 10 1])
    (set-high-tuple! root [2 8 18 1])
    (set-count! root 3)
    (set-block-ref! root (get-id block2))
    (set-tuple-at! block2 0 [2 8 10 1])
    (set-tuple-at! block2 1 [2 8 14 1])
    (set-tuple-at! block2 2 [2 8 18 1])

    (set-low-tuple! rlchild [2 8 20 1])
    (set-high-tuple! rlchild [3 1 1 1])
    (set-count! rlchild 4)
    (set-block-ref! rlchild (get-id block4))
    (set-tuple-at! block4 0 [2 8 20 1])
    (set-tuple-at! block4 1 [2 10 4 1])
    (set-tuple-at! block4 2 [2 20 8 1])
    (set-tuple-at! block4 3 [3 1 1 1])
    
    (set-low-tuple! rchild [3 1 3 1])
    (set-high-tuple! rchild [3 2 1 1])
    (set-count! rchild 2)
    (set-block-ref! rchild (get-id block3))
    (set-tuple-at! block3 0 [3 1 3 1])
    (set-tuple-at! block3 1 [3 2 1 1])
    
    (tree/set-child! root tree/left lchild)
    (tree/set-child! root tree/right rchild)
    (tree/set-child! rchild tree/left rlchild)

    {:root root
     :lchild lchild
     :rchild rchild
     :rlchild rlchild
     :tuple-blocks [block1 block2 block3 block4]
     :tuples tuples}))

(deftest test-tree-build
  ;; this tests that set/get pairs really are working for blocks/nodes
  (let [{:keys [root lchild rchild rlchild tuples] [block1 block2 block3 block4] :tuple-blocks} (create-test-tree)]
    (is (= [2 4 6 1] (get-low-tuple lchild)))
    (is (= [2 8 8 1] (get-high-tuple lchild)))
    (is (= 2 (get-count lchild)))
    (is (= (get-id block1) (get-block-ref lchild)))

    (is (= [2 8 10 1] (get-low-tuple root)))
    (is (= [2 8 18 1] (get-high-tuple root)))
    (is (= 3 (get-count root)))
    (is (= (get-id block2) (get-block-ref root)))))

(deftest tuple-tree-search
  (let [{:keys [root lchild rchild rlchild tuples]
         [block1 block2 block3 block4] :tuple-blocks
         {:keys [index blocks] :as tuples} :tuples} (create-test-tree)]
    (is (= [nil {:node lchild :pos 0}] (find-coord index blocks [2 1 1 1])))
    (is (= [nil {:node lchild :pos 0}] (find-coord index blocks [2 1 1])))

    (is (= {:node lchild :pos 0} (find-coord index blocks [2 4 6 1])))
    (is (= {:node lchild :pos 0} (find-coord index blocks [2 4 6])))
    (is (= [{:node lchild :pos 0} {:node lchild :pos 1}] (find-coord index blocks [2 6 6 1])))
    (is (= [{:node lchild :pos 0} {:node lchild :pos 1}] (find-coord index blocks [2 6 6])))
    (is (= {:node lchild :pos 1} (find-coord index blocks [2 8 8 1])))
    (is (= {:node lchild :pos 1} (find-coord index blocks [2 8 8])))

    (is (= [{:node lchild :pos 1} {:node root :pos 0}] (find-coord index blocks [2 8 9 1])))
    (is (= [{:node lchild :pos 1} {:node root :pos 0}] (find-coord index blocks [2 8 9])))

    (is (= {:node root :pos 0} (find-coord index blocks [2 8 10 1])))
    (is (= {:node root :pos 0} (find-coord index blocks [2 8 10])))
    (is (= [{:node root :pos 0} {:node root :pos 1}] (find-coord index blocks [2 8 12 1])))
    (is (= [{:node root :pos 0} {:node root :pos 1}] (find-coord index blocks [2 8 12])))
    (is (= {:node root :pos 1} (find-coord index blocks [2 8 14 1])))
    (is (= {:node root :pos 1} (find-coord index blocks [2 8 14])))
    (is (= [{:node root :pos 1} {:node root :pos 2}] (find-coord index blocks [2 8 16 1])))
    (is (= [{:node root :pos 1} {:node root :pos 2}] (find-coord index blocks [2 8 16])))
    (is (= {:node root :pos 2} (find-coord index blocks [2 8 18 1])))
    (is (= {:node root :pos 2} (find-coord index blocks [2 8 18])))

    (is (= [{:node root :pos 2} {:node rlchild :pos 0}] (find-coord index blocks [2 8 19 1])))
    (is (= [{:node root :pos 2} {:node rlchild :pos 0}] (find-coord index blocks [2 8 19])))

    (is (= {:node rlchild :pos 0} (find-coord index blocks [2 8 20 1])))
    (is (= {:node rlchild :pos 0} (find-coord index blocks [2 8 20])))
    (is (= [{:node rlchild :pos 0} {:node rlchild :pos 1}] (find-coord index blocks [2 9 12 1])))
    (is (= [{:node rlchild :pos 0} {:node rlchild :pos 1}] (find-coord index blocks [2 9 12])))
    (is (= {:node rlchild :pos 1} (find-coord index blocks [2 10 4 1])))
    (is (= {:node rlchild :pos 1} (find-coord index blocks [2 10 4])))
    (is (= [{:node rlchild :pos 1} {:node rlchild :pos 2}] (find-coord index blocks [2 20 6 1])))
    (is (= [{:node rlchild :pos 1} {:node rlchild :pos 2}] (find-coord index blocks [2 20 6])))
    (is (= {:node rlchild :pos 2} (find-coord index blocks [2 20 8 1])))
    (is (= {:node rlchild :pos 2} (find-coord index blocks [2 20 8])))
    (is (= [{:node rlchild :pos 2} {:node rlchild :pos 3}] (find-coord index blocks [2 20 8 2])))
    (is (= [{:node rlchild :pos 2} {:node rlchild :pos 3}] (find-coord index blocks [2 20 9 1])))
    (is (= [{:node rlchild :pos 2} {:node rlchild :pos 3}] (find-coord index blocks [2 20 9])))
    (is (= {:node rlchild :pos 3} (find-coord index blocks [3 1 1 1])))
    (is (= {:node rlchild :pos 3} (find-coord index blocks [3 1 1])))

    (is (= [{:node rlchild :pos 3} {:node rchild :pos 0}] (find-coord index blocks [3 1 1 2])))
    (is (= [{:node rlchild :pos 3} {:node rchild :pos 0}] (find-coord index blocks [3 1 2])))

    (is (= {:node rchild :pos 0} (find-coord index blocks [3 1 3 1])))
    (is (= {:node rchild :pos 0} (find-coord index blocks [3 1 3])))
    (is (= [{:node rchild :pos 0} {:node rchild :pos 1}] (find-coord index blocks [3 1 4 1])))
    (is (= [{:node rchild :pos 0} {:node rchild :pos 1}] (find-coord index blocks [3 1 4])))
    (is (= {:node rchild :pos 1} (find-coord index blocks [3 2 1 1])))
    (is (= {:node rchild :pos 1} (find-coord index blocks [3 2 1])))

    (is (= [{:node rchild :pos 1} nil] (find-coord index blocks [3 2 2 1])))
    (is (= [{:node rchild :pos 1} nil] (find-coord index blocks [3 2 2])))))

(deftest test-tuple-search
  (let [{:keys [root lchild rchild rlchild tuples]
         [block1 block2 block3 block4] :tuple-blocks
         {:keys [index blocks] :as tuples} :tuples} (create-test-tree)]
    (is (= nil (tuple-seq index blocks [2 1 1] lchild 0)))
    (is (= [[2 8 8 1] [2 8 10 1] [2 8 14 1] [2 8 18 1] [2 8 20 1]] (tuple-seq index blocks [2 8] lchild 1)))
    (is (= [[3 1 1 1]] (tuple-seq index blocks [3 1 1] rlchild 3)))
    (is (= [[2 4 6 1] [2 8 8 1] [2 8 10 1] [2 8 14 1] [2 8 18 1] [2 8 20 1]
            [2 10 4 1] [2 20 8 1] [3 1 1 1] [3 1 3 1] [3 2 1 1]]
           (tuple-seq index blocks [] lchild 0)))))

(deftest test-delete
  (let [{:keys [root lchild rchild rlchild tuples]
         [block1 block2 block3 block4] :tuple-blocks
         {:keys [index blocks]} :tuples} (create-test-tree)
        tuples (->TupleIndex index blocks (get-id root))
        tuples (delete-tuple! tuples [2 8 14 1])
        _ (is (= [[2 4 6 1] [2 8 8 1] [2 8 10 1] [2 8 18 1] [2 8 20 1]
                  [2 10 4 1] [2 20 8 1] [3 1 1 1] [3 1 3 1] [3 2 1 1]]
                 (tuple-seq index blocks [] lchild 0)))
        ;; this triple does not exist
        tuples (delete-tuple! tuples [2 8 9 1])
        _ (is (= [[2 4 6 1] [2 8 8 1] [2 8 10 1] [2 8 18 1] [2 8 20 1]
                  [2 10 4 1] [2 20 8 1] [3 1 1 1] [3 1 3 1] [3 2 1 1]]
                 (tuple-seq index blocks [] lchild 0)))
        tuples (delete-tuple! tuples [2 8 10 1])
        _ (is (= [[2 4 6 1] [2 8 8 1] [2 8 18 1] [2 8 20 1]
                  [2 10 4 1] [2 20 8 1] [3 1 1 1] [3 1 3 1] [3 2 1 1]]
                 (tuple-seq index blocks [] lchild 0)))
        tuples (delete-tuple! tuples [2 8 18 1])
        _ (is (= [[2 4 6 1] [2 8 8 1] [2 8 20 1]
                  [2 10 4 1] [2 20 8 1] [3 1 1 1] [3 1 3 1] [3 2 1 1]]
                 (tuple-seq index blocks [] lchild 0)))
        tuples (delete-tuple! tuples [2 4 6 1])
        _ (is (= [[2 8 8 1] [2 8 20 1]
                  [2 10 4 1] [2 20 8 1] [3 1 1 1] [3 1 3 1] [3 2 1 1]]
                 (tuple-seq index blocks [] lchild 0)))
        tuples (delete-tuple! tuples [2 8 8 1])
        ;; tuple-seq works. Switch to find-tuple
        _ (is (= [[2 8 20 1]
                  [2 10 4 1] [2 20 8 1] [3 1 1 1] [3 1 3 1] [3 2 1 1]]
                 (find-tuple tuples [])))
        tuples (delete-tuple! tuples [3 1 1 1])
        _ (is (= [[2 8 20 1]
                  [2 10 4 1] [2 20 8 1] [3 1 3 1] [3 2 1 1]]
                 (find-tuple tuples [])))
        tuples (delete-tuple! tuples [3 1 3 1])
        _ (is (= [[2 8 20 1] [2 10 4 1] [2 20 8 1] [3 2 1 1]]
                 (find-tuple tuples [])))
        tuples (delete-tuple! tuples [3 2 1 1])
        _ (is (= [[2 8 20 1] [2 10 4 1] [2 20 8 1]]
                 (find-tuple tuples [])))
        tuples (delete-tuple! tuples [2 20 8 1])
        _ (is (= [[2 8 20 1] [2 10 4 1]]
                 (find-tuple tuples [])))
        tuples (delete-tuple! tuples [2 10 4 1])
        _ (is (= [[2 8 20 1]] (find-tuple tuples [])))
        tuples (delete-tuple! tuples [2 8 20 1])]
    (is (= nil (find-tuple tuples [])))))


(defn create-test-tuples
  []
  (let [node-blocks (faux-manager tree-block-size)
        ;; allocate the NULL block
        null (allocate-block! node-blocks)
        _ (assert (zero? (get-id null)))
        index (tree/new-block-tree
               (constantly node-blocks)
               "" tree-node-size tuple-node-compare)
        tuple-blocks (faux-manager tuples-block-size)]
    (->TupleIndex index tuple-blocks nil)))

(deftest test-small-insert
  (let [tuples (create-test-tuples)
        tuples (write-tuple! tuples [1 1 1 1])
        _ (is (= [[1 1 1 1]] (find-tuple tuples [])))
        tuples (write-tuple! tuples [1 1 3 1])
        _ (is (= [[1 1 1 1] [1 1 3 1]] (find-tuple tuples [])))
        tuples (write-tuple! tuples [1 1 2 1])
        _ (is (= [[1 1 1 1] [1 1 2 1] [1 1 3 1]] (find-tuple tuples [])))
        tuples (write-tuple! tuples [1 1 0 1])
        _ (is (= [[1 1 0 1] [1 1 1 1] [1 1 2 1] [1 1 3 1]] (find-tuple tuples [])))
        tuples (write-tuple! tuples [1 1 4 1])
        _ (is (= [[1 1 0 1] [1 1 1 1] [1 1 2 1] [1 1 3 1] [1 1 4 1]] (find-tuple tuples [])))]

    (is (nil? (write-tuple! tuples [1 1 1 1])))
    (is (nil? (write-tuple! tuples [1 1 3 1])))
    (is (nil? (write-tuple! tuples [1 1 2 1])))
    (is (nil? (write-tuple! tuples [1 1 0 1])))
    (is (nil? (write-tuple! tuples [1 1 4 1])))
    (let [tuples (write-tuple! tuples [1 1 5 1])]
      (is (= [[1 1 0 1] [1 1 1 1] [1 1 2 1] [1 1 3 1] [1 1 4 1] [1 1 5 1]] (find-tuple tuples [])))

      (is (nil? (write-tuple! tuples [1 1 2 1])))
      (is (nil? (write-new-tx-tuple! tuples [1 1 2 2])))
      (is (nil? (write-new-tx-tuple! tuples [1 1 4 2])))
      
      (is (nil? (write-new-tx-tuple! tuples [1 1 3 2])))
      (is (nil? (write-new-tx-tuple! tuples [1 1 0 2])))
      (let [tuples (write-new-tx-tuple! tuples [1 1 6 2])]
        (is (nil? (write-new-tx-tuple! tuples [1 1 6 2])))
        (is (nil? (write-new-tx-tuple! tuples [1 1 6 1])))
        (is (= [[1 1 0 1] [1 1 1 1] [1 1 2 1] [1 1 3 1] [1 1 4 1] [1 1 5 1] [1 1 6 2]] (find-tuple tuples [])))))))

(defn tnodes
  [{:keys [index blocks] :as tuples}]
  (let [c (find-coord index blocks [])]
    (tree/node-seq index (:node c))))

(deftest test-split-insert
  (let [tuples (create-test-tuples)
        data (map #(vector 2 2 % 2) (range block-max))
        tuples (reduce write-tuple! tuples data)
        _ (is (= data (find-tuple tuples [])))
        _ (is (= 1 (count (tnodes tuples))))
        tuples (write-tuple! tuples [1 1 1 1])
        [n1 n2 :as nodes] (tnodes tuples)]
    (is (= (cons [1 1 1 1] data) (find-tuple tuples [])))
    (is (= 2 (count nodes)))
    (is (= [1 1 1 1] (get-low-tuple n1)))
    (is (= [2 2 255 2] (get-high-tuple n1)))
    (is (= 257 (get-count n1)))
    (is (= [2 2 256 2] (get-low-tuple n2)))
    (is (= [2 2 511 2] (get-high-tuple n2)))
    (is (= 256 (get-count n2))))

  (let [tuples (create-test-tuples)
        data (map #(vector 2 2 % 2) (range block-max))
        tuples (reduce write-tuple! tuples data)
        tuples (write-tuple! tuples [2 3 1 1])
        [n1 n2 :as nodes] (tnodes tuples)]
    (is (= (concat data [[2 3 1 1]]) (find-tuple tuples [])))
    (is (= 2 (count nodes)))
    (is (= [2 2 0 2] (get-low-tuple n1)))
    (is (= [2 2 255 2] (get-high-tuple n1)))
    (is (= 256 (get-count n1)))
    (is (= [2 2 256 2] (get-low-tuple n2)))
    (is (= [2 3 1 1] (get-high-tuple n2)))
    (is (= 257 (get-count n2))))

  (let [tuples (create-test-tuples)
        data (map #(vector 2 2 % 2) (range block-max))
        tuples (reduce write-tuple! tuples data)
        tuples (write-tuple! tuples [2 2 255 3])
        [n1 n2 :as nodes] (tnodes tuples)]
    (is (= (concat (take 256 data) [[2 2 255 3]] (drop 256 data)) (find-tuple tuples [])))
    (is (= 2 (count nodes)))
    (is (= [2 2 0 2] (get-low-tuple n1)))
    (is (= [2 2 255 3] (get-high-tuple n1)))
    (is (= 257 (get-count n1)))
    (is (= [2 2 256 2] (get-low-tuple n2)))
    (is (= [2 2 511 2] (get-high-tuple n2)))
    (is (= 256 (get-count n2))))

  (let [tuples (create-test-tuples)
        data (map #(vector 2 2 % 2) (range block-max))
        tuples (reduce write-tuple! tuples data)
        tuples (write-tuple! tuples [2 2 256 3])
        [n1 n2 :as nodes] (tnodes tuples)]
    (is (= (concat (take 257 data) [[2 2 256 3]] (drop 257 data)) (find-tuple tuples [])))
    (is (= 2 (count nodes)))
    (is (= [2 2 0 2] (get-low-tuple n1)))
    (is (= [2 2 255 2] (get-high-tuple n1)))
    (is (= 256 (get-count n1)))
    (is (= [2 2 256 2] (get-low-tuple n2)))
    (is (= [2 2 511 2] (get-high-tuple n2)))
    (is (= 257 (get-count n2)))))

;; forces 2 splits and a rebalance
(deftest test-rebalance-split
  (let [tuples (create-test-tuples)
        data (map #(vector 2 % 2 2) (range block-max))
        tuples (reduce write-tuple! tuples data)
        d (map #(vector 2 256 % 2) (range (inc (- block-max)) 1))
        d2 (interleave (take 256 d) (reverse (drop 256 d)))
        tuples (reduce write-tuple! tuples d2)
        [n1 n2 :as nodes] (tnodes tuples)]
    (is (= (concat (take 256 data) d (drop 256 data)) (find-tuple tuples [])))
    (is (= 2 (count (tnodes tuples))))
    (is (= [2 0 2 2] (get-low-tuple n1)))
    (is (= [2 256 -256 2] (get-high-tuple n1)))
    (is (= 512 (get-count n1)))
    (is (= [2 256 -255 2] (get-low-tuple n2)))
    (is (= [2 511 2 2] (get-high-tuple n2)))
    (is (= 512 (get-count n2)))
    (let [tuples (write-tuple! tuples [2 256 -256 3])
          [n1 n2 n3 :as nodes] (tnodes tuples)]
      (is (= (concat (take 256 data) (take 256 d)
                     [[2 256 -256 3]]
                     (drop 256 d) (drop 256 data))
             (find-tuple tuples [])))
      (is (= 3 (count nodes)))
      (is (= [2 0 2 2] (get-low-tuple n1)))
      (is (= [2 255 2 2] (get-high-tuple n1)))
      (is (= 256 (get-count n1)))
      (is (= [2 256 -511 2] (get-low-tuple n2)))
      (is (= [2 256 -256 3] (get-high-tuple n2)))
      (is (= 257 (get-count n2)))
      (is (= [2 256 -255 2] (get-low-tuple n3)))
      (is (= [2 511 2 2] (get-high-tuple n3)))
      (is (= 512 (get-count n3)))
      )
    ))
