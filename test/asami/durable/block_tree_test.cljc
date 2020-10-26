(ns ^{:doc "Testing AVL trees built with blocks"
      :author "Paula Gearon"}
    asami.durable.block-tree-test
  (:require [asami.durable.tree :refer [header-size left-offset right-offset left-mask null left right
                                        new-block-tree find-node add get-node-block get-child
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

(defn long-compare
  [a b-block]
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
;;   2
;;  /
;; 1
(deftest insert-left
  (let [bm (create-block-manager "two.avl" (+ header-size Long/BYTES))
        empty-tree (new-block-tree bm nil long-compare)
        tree (add empty-tree 2 long-writer)
        tree (add tree 1 long-writer)
        f0 (find-node tree 0)
        f1 (find-node tree 1)
        f2 (find-node tree 2)
        f3 (find-node tree 3)]
    (is (= [nil f1] f0))

    (is (= 1 (get-data f1)))
    (is (= 0 (get-balance f1)))
    (is (= null (get-left-id f1)))
    (is (= null (get-right-id f1)))

    (is (= f2 (:root tree)))
    (is (= 2 (get-data f2)))
    (is (= left (get-balance f2)))
    (is (= (get-node-id f1) (get-left-id f2)))
    (is (= null (get-right-id f2)))

    (is (= [f2 nil] f3))))

;; build a tree of
;;   2
;;  / \
;; 1   3
(deftest insert-nodes
  (let [bm (create-block-manager "three.avl" (+ header-size Long/BYTES))
        empty-tree (new-block-tree bm nil long-compare)
        tree (add empty-tree 2 long-writer)
        tree (add tree 1 long-writer)
        tree (add tree 3 long-writer)
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

    (is (= f2 (:root tree)))
    (is (= 2 (get-data f2)))
    (is (= 0 (get-balance f2)))
    (is (= (get-node-id f1) (get-left-id f2)))
    (is (= (get-node-id f3) (get-right-id f2)))

    (is (= 3 (get-data f3)))
    (is (= 0 (get-balance f3)))
    (is (= null (get-left-id f3)))
    (is (= null (get-right-id f3)))

    (is (= [f3 nil] f4))))


(defn print-structure
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
      (print-node r))))

;; build a tree of
;;     3
;;    /
;;   2
;;  /
;; 1
;; This is LL heavy and rebalances with a Right Rotation
(deftest rotate-right
  (let [bm (create-block-manager "ll.avl" (+ header-size Long/BYTES))
        empty-tree (new-block-tree bm nil long-compare)
        tree (add empty-tree 3 long-writer)
        tree (add tree 2 long-writer)
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

    (is (= f2 (:root tree)))
    (is (= 2 (get-data f2)))
    (is (= 0 (get-balance f2)))
    (is (= (get-node-id f1) (get-left-id f2)))
    (is (= (get-node-id f3) (get-right-id f2)))

    (is (= 3 (get-data f3)))
    (is (= 0 (get-balance f3)))
    (is (= null (get-left-id f3)))
    (is (= null (get-right-id f3)))

    (is (= [f3 nil] f4))))
