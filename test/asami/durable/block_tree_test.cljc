(ns ^{:doc "Testing AVL trees built with blocks"
      :author "Paula Gearon"}
    asami.durable.block-tree-test
  (:require [asami.durable.tree :refer [header-size new-block-tree find-node add get-node-block]]
            [asami.durable.test-utils :refer [get-filename]]
            [asami.durable.block.block-api :refer [get-long put-long!]]
            [clojure.test :refer [deftest is]]
            #?(:clj [asami.durable.block.file.block-file :refer [create-managed-block-file]])))

(defn create-block-manager
  "Creates a block manager"
  [name block-size]
  #?(:clj
     (create-managed-block-file (get-filename name) block-size)

     :cljs
     ;; TODO: ClojureScript block manager
     ))

(defn long-compare
  [a b-block]
  (compare a (get-long b-block header-size)))

(defn long-writer
  [block hdr i]
  (put-long! block hdr i))

(deftest create-tree
  (let [bm (create-block-manager "create.avl" (+ header-size Long/BYTES))
        empty-tree (new-block-tree bm nil long-compare)
        f (find-node empty-tree 1)]
    (is (nil? f))))

(deftest insert-node
  (let [bm (create-block-manager "one.avl" (+ header-size Long/BYTES))
        empty-tree (new-block-tree bm nil long-compare)
        tree (add empty-tree 1 long-writer)
        f (find-node tree 1)]
    (is (= f (:root tree)))
    (is (= 1 (get-long (get-node-block f) header-size)))))

