(ns ^{:doc "Testing AVL trees built with blocks"
      :author "Paula Gearon"}
    asami.durable.block-tree-test
  (:require [asami.durable.tree :refer [header-size left-offset right-offset left-mask null left right
                                        new-block-tree find-node add get-node-block get-child node-seq
                                        get-node-id get-balance]]
            [asami.durable.test-utils :refer [get-filename]]
            [asami.durable.block.block-api :refer [get-long put-long! get-id]]
            [clojure.test :refer [deftest is]]
            #?(:clj [asami.durable.block.file.block-file :refer [create-managed-block-file]])))

(defn create-block-manager
  "Creates a block manager"
  [name block-size]
  #?(:clj
     (create-managed-block-file (get-filename name) block-size)

     :cljs
     ;; TODO: create ClojureScript block manager
     ))

(def cntr (atom nil))
(defn long-compare
  [a b-block]
  #_(when @cntr
    (println (str "(compare " a " " (get-long b-block header-size) ") => " (compare a (get-long b-block header-size))))
    (if (> (swap! cntr inc) 10000)
      (throw (ex-info "Too deep" {}))))
  (compare a (get-long b-block header-size)))

(defn long-writer
  [block hdr i]
  (put-long! block hdr i))

;; utilities for looking in blocks
(defn get-left-id
  [node]
  (bit-and left-mask (get-long (get-node-block node) left-offset)))

(defn get-right-id
  [node]
  (get-long (get-node-block node) right-offset))

(defn get-data
  [node]
  (get-long (get-node-block node) header-size))

(deftest create-tree
  (let [bm (create-block-manager "create.avl" (+ header-size Long/BYTES))
        empty-tree (new-block-tree bm nil long-compare)
        f (find-node empty-tree 1)]
    (is (nil? f))))

(deftest insert-node
  (let [bm (create-block-manager "one.avl" (+ header-size Long/BYTES))
        empty-tree (new-block-tree bm nil long-compare)
        tree (add empty-tree 2 long-writer)
        f1 (find-node tree 1)
        f2 (find-node tree 2)
        f3 (find-node tree 3)]
    (is (= [nil f2] f1))
    (is (= f2 (:root tree)))
    (is (= 2 (get-data f2)))
    (is (= 0 (get-balance f2)))
    (is (= [f2 nil] f3))))

;; build a tree of
;;   3
;;  /
;; 1
(deftest insert-left
  (let [bm (create-block-manager "two.avl" (+ header-size Long/BYTES))
        empty-tree (new-block-tree bm nil long-compare)
        tree (add empty-tree 3 long-writer)
        tree (add tree 1 long-writer)
        f0 (find-node tree 0)
        f1 (find-node tree 1)
        f2 (find-node tree 2)
        f3 (find-node tree 3)
        f4 (find-node tree 4)]
    (is (= [nil f1] f0))

    (is (= 1 (get-data f1)))
    (is (= 0 (get-balance f1)))
    (is (= null (get-left-id f1)))
    (is (= null (get-right-id f1)))

    (is (= [f1 f3] f2))

    (is (= f3 (:root tree)))
    (is (= 3 (get-data f3)))
    (is (= left (get-balance f3)))
    (is (= (get-node-id f1) (get-left-id f3)))
    (is (= null (get-right-id f3)))

    (is (= [f3 nil] f4))))

;; build a tree of
;;   3
;;  / \
;; 1   5
(deftest insert-nodes
  (let [bm (create-block-manager "three.avl" (+ header-size Long/BYTES))
        empty-tree (new-block-tree bm nil long-compare)
        tree (add empty-tree 3 long-writer)
        tree (add tree 1 long-writer)
        tree (add tree 5 long-writer)
        f0 (find-node tree 0)
        f1 (find-node tree 1)
        f2 (find-node tree 2)
        f3 (find-node tree 3)
        f4 (find-node tree 4)
        f5 (find-node tree 5)
        f6 (find-node tree 6)]
    (is (= [nil f1] f0))

    (is (= 1 (get-data f1)))
    (is (= 0 (get-balance f1)))
    (is (= null (get-left-id f1)))
    (is (= null (get-right-id f1)))

    (is (= [f1 f3] f2))

    (is (= f3 (:root tree)))
    (is (= 3 (get-data f3)))
    (is (= 0 (get-balance f3)))
    (is (= (get-node-id f1) (get-left-id f3)))
    (is (= (get-node-id f5) (get-right-id f3)))

    (is (= [f3 f5] f4))

    (is (= 5 (get-data f5)))
    (is (= 0 (get-balance f5)))
    (is (= null (get-left-id f5)))
    (is (= null (get-right-id f5)))

    (is (= [f5 nil] f6))))

(defn triple-node-test
  [bm vs]
  (let [empty-tree (new-block-tree bm nil long-compare)
        tree (reduce (fn [t v] (add t v long-writer)) empty-tree vs)
        [f0 f1 f2 f3 f4 f5 f6] (map (partial find-node tree) (range 7))]

    (is (= [nil f1] f0))

    (is (= 1 (get-data f1)))
    (is (= 0 (get-balance f1)))
    (is (= null (get-left-id f1)))
    (is (= null (get-right-id f1)))

    (is (= [f1 f3] f2))

    (is (= f3 (:root tree)))
    (is (= 3 (get-data f3)))
    (is (= 0 (get-balance f3)))
    (is (= (get-node-id f1) (get-left-id f3)))
    (is (= (get-node-id f5) (get-right-id f3)))

    (is (= [f3 f5] f4))

    (is (= 5 (get-data f5)))
    (is (= 0 (get-balance f5)))
    (is (= null (get-left-id f5)))
    (is (= null (get-right-id f5)))

    (is (= [f5 nil] f6))))

;; build a tree of
;;     5
;;    /
;;   3
;;  /
;; 1
;; This is LL heavy and rebalances with a Right Rotation
(deftest rotate-right
  (let [bm (create-block-manager "ll.avl" (+ header-size Long/BYTES))]
    (triple-node-test bm [5 3 1])))

;; build a tree of
;;     5
;;    /
;;   1
;;    \
;;     3
;; This is LR heavy and rebalances with a Double Rotation
(deftest double-rotate-right
  (let [bm (create-block-manager "lr.avl" (+ header-size Long/BYTES))]
    (triple-node-test bm [5 1 3])))

;; build a tree of
;; 1
;;  \
;;   3
;;    \
;;     5
;; This is RR heavy and rebalances with a Left Rotation
(deftest rotate-left
  (let [bm (create-block-manager "rr.avl" (+ header-size Long/BYTES))]
    (triple-node-test bm [1 3 5])))

;; build a tree of
;;     1
;;      \
;;       5
;;      /
;;     3
;; This is RL heavy and rebalances with a Double Rotation
(deftest double-rotate-left
  (let [bm (create-block-manager "rl.avl" (+ header-size Long/BYTES))]
    (triple-node-test bm [1 5 3])))


(defn five-node-left-test
  [bm vs]
  (let [empty-tree (new-block-tree bm nil long-compare)
        tree (reduce (fn [t v] (add t v long-writer)) empty-tree vs)
        [f0 f1 f2 f3 f4 f5 f6 f7 f8 f9 f10] (map (partial find-node tree) (range 11))]

    (is (= [nil f1] f0))

    (is (= 1 (get-data f1)))
    (is (= 0 (get-balance f1)))
    (is (= null (get-left-id f1)))
    (is (= null (get-right-id f1)))

    (is (= [f1 f3] f2))

    (is (= 3 (get-data f3)))
    (is (= 0 (get-balance f3)))
    (is (= (get-node-id f1) (get-left-id f3)))
    (is (= (get-node-id f5) (get-right-id f3)))

    (is (= [f3 f5] f4))

    (is (= 5 (get-data f5)))
    (is (= 0 (get-balance f5)))
    (is (= null (get-left-id f5)))
    (is (= null (get-right-id f5)))

    (is (= [f5 f7] f6))

    (is (= f7 (:root tree)))
    (is (= 7 (get-data f7)))
    (is (= left (get-balance f7)))
    (is (= (get-node-id f3) (get-left-id f7)))
    (is (= (get-node-id f9) (get-right-id f7)))

    (is (= [f7 f9] f8))
    (is (= 9 (get-data f9)))
    (is (= 0 (get-balance f9)))
    (is (= null (get-left-id f9)))
    (is (= null (get-right-id f9)))

    (is (= [f9 nil] f10))))

;; build a tree of
;;       7
;;      / \
;;     5   9
;;    /
;;   3
;;  /
;; 1
;; This is LL heavy and rebalances with a Right Rotation
(deftest left-deep-rotate-right
  (let [bm (create-block-manager "ldll.avl" (+ header-size Long/BYTES))]
    (five-node-left-test bm [7 9 5 3 1])))

;; build a tree of
;;       7
;;      / \
;;     1   9
;;      \
;;       3
;;        \
;;         5
;; This is RR heavy and rebalances with a Left Rotation
(deftest left-deep-rotate-left
  (let [bm (create-block-manager "ldrr.avl" (+ header-size Long/BYTES))]
    (five-node-left-test bm [7 9 1 3 5])))

;; build a tree of
;;       7
;;      / \
;;     5   9
;;    /
;;   1
;;    \
;;     3
;; This is LR heavy and rebalances with a double Rotation
(deftest left-deep-double-rotate-right
  (let [bm (create-block-manager "ldlr.avl" (+ header-size Long/BYTES))]
    (five-node-left-test bm [7 9 5 1 3])))

;; build a tree of
;;       7
;;      / \
;;     1   9
;;      \
;;       5
;;      /
;;     3
;; This is RL heavy and rebalances with a double Rotation
(deftest left-deep-double-rotate-left
  (let [bm (create-block-manager "ldrl.avl" (+ header-size Long/BYTES))]
    (five-node-left-test bm [7 9 1 5 3])))


(defn five-node-right-test
  [bm vs]
  (let [empty-tree (new-block-tree bm nil long-compare)
        tree (reduce (fn [t v] (add t v long-writer)) empty-tree vs)
        [f0 f1 f2 f3 f4 f5 f6 f7 f8 f9 f10] (map (partial find-node tree) (range 11))]

    (is (= [nil f1] f0))

    (is (= 1 (get-data f1)))
    (is (= 0 (get-balance f1)))
    (is (= null (get-left-id f1)))
    (is (= null (get-right-id f1)))

    (is (= [f1 f3] f2))

    (is (= f3 (:root tree)))
    (is (= 3 (get-data f3)))
    (is (= right (get-balance f3)))
    (is (= (get-node-id f1) (get-left-id f3)))
    (is (= (get-node-id f7) (get-right-id f3)))

    (is (= [f3 f5] f4))

    (is (= 5 (get-data f5)))
    (is (= 0 (get-balance f5)))
    (is (= null (get-left-id f5)))
    (is (= null (get-right-id f5)))

    (is (= [f5 f7] f6))

    (is (= 7 (get-data f7)))
    (is (= 0 (get-balance f7)))
    (is (= (get-node-id f5) (get-left-id f7)))
    (is (= (get-node-id f9) (get-right-id f7)))

    (is (= [f7 f9] f8))
    (is (= 9 (get-data f9)))
    (is (= 0 (get-balance f9)))
    (is (= null (get-left-id f9)))
    (is (= null (get-right-id f9)))

    (is (= [f9 nil] f10))))

;; build a tree of
;;       3
;;      / \
;;     1   9
;;        /
;;       7
;;      /
;;     5
;; This is LL heavy and rebalances with a Right Rotation
(deftest right-deep-rotate-right
  (let [bm (create-block-manager "rdll.avl" (+ header-size Long/BYTES))]
    (five-node-right-test bm [3 1 9 7 5])))

;; build a tree of
;;       3
;;      / \
;;     1   5
;;          \
;;           7
;;            \
;;             9
;; This is RR heavy and rebalances with a Left Rotation
(deftest right-deep-rotate-left
  (let [bm (create-block-manager "rdrr.avl" (+ header-size Long/BYTES))]
    (five-node-right-test bm [3 1 5 7 9])))

;; build a tree of
;;       3
;;      / \
;;     1   9
;;        /
;;       5
;;        \
;;         7
;; This is LR heavy and rebalances with a double Rotation
(deftest right-deep-double-rotate-right
  (let [bm (create-block-manager "rdlr.avl" (+ header-size Long/BYTES))]
    (five-node-right-test bm [3 9 1 5 7])))

;; build a tree of
;;       3
;;      / \
;;     1   5
;;          \
;;           9
;;          /
;;         7
;; This is RL heavy and rebalances with a double Rotation
(deftest right-deep-double-rotate-left
  (let [bm (create-block-manager "rdrl.avl" (+ header-size Long/BYTES))]
    (five-node-right-test bm [3 5 1 9 7])))


(def pseudo-random (map #(bit-shift-right % 2) (take 1024 (iterate (fn [x] (mod (* 53 x) 4096)) 113))))

(deftest large-tree
  (let [bm (create-block-manager "large.avl" (+ header-size Long/BYTES))
        empty-tree (new-block-tree bm nil long-compare)
        c (volatile! 0)
        tree (reduce (fn [t v]
                       (println (str "insert #" (vswap! c inc)))
                       (add t v long-writer))
                     empty-tree pseudo-random)
        _ (println "INSERTED")
        node0 (find-node tree 0)]
    (is (= (range 1024) (node-seq bm node0)))))

(comment
  (defn print-structure
    "This function is only used for debugging the tree structure"
    [tree bm]
    (letfn [(print-node [n]
              (println ">>> ID:" (get-node-id n) " DATA:" (get-data n))
              (let [l (get-left-id n)
                    r (get-right-id n)]
                (println "LEFT/RIGHT: " l "/" r)
                (when-not (= null l)
                  (println "LEFT")
                  (print-node (get-child n bm left)))
                (when-not (= null r)
                  (println "RIGHT")
                  (print-node (get-child n bm right)))
                (println "----end" (get-node-id n))))]

      (let [r (:root tree)]
        (println "^^^^^^^^^")
        (println "ROOT DATA: " (get-data r))
        (println "ROOT ID: " (get-node-id r))
        (println)
        (print-node r)))))


