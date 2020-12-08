(ns ^{:doc "Tests for the tuples store"
      :author "Paula Gearon"}
    asami.durable.tuples-test
  (:require [clojure.test :refer [deftest is]]
            [asami.durable.test-utils :refer [new-block]]
            [asami.durable.common :refer [long-size]]
            [asami.durable.tree :as tree]
            [asami.durable.tuples :refer [search-block tuple-size tree-node-size block-max
                                          set-low-tuple! set-high-tuple!
                                          set-count! set-block-ref!
                                          tuple-at set-tuple-at!]]
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

(defrecord FauxManager [block-list]
  BlockManager
  (allocate-block! [this]
    (let [b (new-block tuples-block-size (count @block-list))]
      (swap! block-list conj b)
      b))
  (copy-block! [this block] (throw (ex-info "Unsupported operation on test stub" {:op "copy-block!"})))
  (write-block [this block] (throw (ex-info "Unsupported operation on test stub" {:op "write-block"})))
  (get-block [this id] (nth @block-list id))
  (get-block-size [this] tuples-block-size)
  (copy-to-tx [this block] (throw (ex-info "Unsupported operation on test stub" {:op "copy-to-tx"}))))

(defn faux-manager [] (->FauxManager (atom [])))

(deftest tuple-tree-search
  (let [root (tree/->Node (new-block tree-block-size) nil)
        lchild (tree/->Node (new-block tree-block-size) root)
        rchild (tree/->Node (new-block tree-block-size) root)
        blocks (faux-manager)
        block1 (allocate-block! blocks)
        block2 (allocate-block! blocks)
        block3 (allocate-block! blocks)]
    (set-low-tuple! lchild [2 4 6 1])
    (set-high-tuple! lchild [2 8 8 1])
    (set-count! lchild 2)
    (set-block-ref! lchild (get-id block1))
    (set-tuple-at! block1 0 [2 4 6 1])
    (set-tuple-at! block1 1 [2 8 8 1])
    
    (set-low-tuple! root [2 8 10 1])
    (set-high-tuple! root [2 8 18 1])
    (set-count! lchild 3)
    (set-block-ref! lchild (get-id block2))
    (set-tuple-at! block2 0 [2 8 10 1])
    (set-tuple-at! block2 1 [2 8 14 1])
    (set-tuple-at! block2 2 [2 8 18 1])
    
    (set-low-tuple! root [2 8 20 1])
    (set-high-tuple! root [3 1 1 1])
    (set-count! lchild 4)
    (set-block-ref! lchild (get-id block3))
    (set-tuple-at! block2 0 [2 8 20 1])
    (set-tuple-at! block2 1 [2 10 4 1])
    (set-tuple-at! block2 2 [2 20 8 1])
    (set-tuple-at! block2 3 [3 1 1 1])
    
    (tree/set-child! root tree/left lchild)
    (tree/set-child! root tree/right rchild)))
