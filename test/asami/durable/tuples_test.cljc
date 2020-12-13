(ns ^{:doc "Tests for the tuples store"
      :author "Paula Gearon"}
    asami.durable.tuples-test
  (:require [clojure.test :refer [deftest is]]
            [asami.durable.test-utils :refer [new-block]]
            [asami.durable.common :refer [long-size]]
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
            [asami.durable.block.block-api :refer [BlockManager allocate-block! get-id get-long put-long!]]))

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

(defrecord FauxManager [block-list block-size]
  BlockManager
  (allocate-block! [this]
    (let [b (new-block block-size (count @block-list))]
      (swap! block-list conj b)
      b))
  (copy-block! [this block] (throw (ex-info "Unsupported operation on test stub" {:op "copy-block!"})))
  (write-block [this block] this)
  (get-block [this id] (nth @block-list id))
  (get-block-size [this] tuples-block-size)
  (copy-to-tx [this block] this))

(defn faux-manager [s] (->FauxManager (atom []) s))

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
     :tuples tuples}
    ))

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
