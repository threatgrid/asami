(ns ^{:doc "Testing AVL trees built with blocks"
      :author "Paula Gearon"}
    asami.durable.block-tree
  (:require [asami.durable.tree :refer [header-size new-block-tree nfind]]
            [asami.durable.test-utils :refer [get-filename]]
            [asami.durable.block.block-api :refer [get-long]]
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

(deftest create-tree
  (let [bm (create-block-manager "create.avl" (+ header-size Long/BYTES))
        empty-tree (new-block-tree bm nil long-compare)
        f (nfind empty-tree 1)]
    (is (nil? f))))
