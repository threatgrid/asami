(ns ^{:doc "Testing AVL trees built with blocks"
      :author "Paula Gearon"}
    asami.durable.block-tree-test
  (:require [asami.durable.tree :refer [header-size left-offset right-offset left-mask null left right
                                        new-block-tree find-node add get-child node-seq get-node
                                        first-node last-node
                                        get-balance get-child-id at]]
            [asami.durable.test-utils :refer [get-filename]]
            [asami.durable.common :refer [close rewind! commit! long-size]]
            [asami.durable.block.block-api :refer [get-long put-long! get-id]]
            [clojure.test #?(:clj :refer :cljs :refer-macros) [deftest is]]
            #?(:clj [asami.durable.block.file.util :as util])
            #?(:clj [asami.durable.block.file.block-file :refer [create-managed-block-file]])))

(defn create-block-manager
  "Creates a block manager. If the reuse? flag is on, then an existing non-empty block manager is returned.
  Otherwise the block manager has only fresh blocks."
  ([] true)
  ([name block-size & [reuse?]]
   #?(:clj
      ;; get-filename will ensure that a file is EMPTY.
      ;; to reuse a temporary file, will need to find it directly with temp-file
      (let [f (if reuse?
                (util/temp-file name)
                (get-filename name))]
        (create-managed-block-file f block-size))

      :cljs
      ;; TODO: create ClojureScript block manager
      nil
      )))

(defn reopen-block-manager
  ([] true)
  ([name block-size]
   (create-block-manager name block-size true)))

(defn long-compare
  [a node]
  (compare a (get-long node 0)))

(defn long-writer
  [node i]
  (put-long! node 0 i))

;; utilities for looking in blocks
(defn get-left-id
  [node]
  (bit-and left-mask (get-long (:block node) left-offset)))

(defn get-right-id
  [node]
  (get-long (:block node) right-offset))

(defn get-data
  [node]
  (get-long node 0))

(defn node-str
  [node]
  (if node
    (str (get-data node) " [id:" (get-id node) "]  L:" (get-child-id node left) "  R:" (get-child-id node right)
         "  balance: " (get-balance node) " parent: " (if-let [p (:parent node)] (str "id: " (get-id p)) "NULL"))
    "NULL"))

#?(:clj
   (deftest create-tree
     (let [empty-tree (new-block-tree create-block-manager "create.avl" long-size long-compare)
           f (find-node empty-tree 1)]
       (is (nil? f)))))

#?(:clj
   (deftest insert-node
     (let [empty-tree (new-block-tree create-block-manager "one.avl" long-size long-compare)
           tree (add empty-tree 2 long-writer)
           f1 (find-node tree 1)
           f2 (find-node tree 2)
           f3 (find-node tree 3)]
       (is (= [nil f2] f1))
       (is (= f2 (:root tree)))
       (is (= 2 (get-data f2)))
       (is (= 0 (get-balance f2)))
       (is (= [f2 nil] f3)))))

;; build a tree of
;;   3
;;  /
;; 1
#?(:clj
   (deftest insert-left
     (let [empty-tree (new-block-tree create-block-manager "two.avl" long-size long-compare)
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
       (is (= (get-id f1) (get-left-id f3)))
       (is (= null (get-right-id f3)))

       (is (= [f3 nil] f4)))))

;; build a tree of
;;   3
;;  / \
;; 1   5
#?(:clj
   (deftest insert-nodes
     (let [empty-tree (new-block-tree create-block-manager "three.avl" long-size long-compare)
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
       (is (= (get-id f1) (get-left-id f3)))
       (is (= (get-id f5) (get-right-id f3)))

       (is (= [f3 f5] f4))

       (is (= 5 (get-data f5)))
       (is (= 0 (get-balance f5)))
       (is (= null (get-left-id f5)))
       (is (= null (get-right-id f5)))

       (is (= [f5 nil] f6)))))

#?(:clj
   (defn triple-node-test
     [nm vs]
     (let [empty-tree (new-block-tree create-block-manager nm long-size long-compare)
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
       (is (= (get-id f1) (get-left-id f3)))
       (is (= (get-id f5) (get-right-id f3)))

       (is (= [f3 f5] f4))

       (is (= 5 (get-data f5)))
       (is (= 0 (get-balance f5)))
       (is (= null (get-left-id f5)))
       (is (= null (get-right-id f5)))

       (is (= [f5 nil] f6))
       (close tree))))

;; build a tree of
;;     5
;;    /
;;   3
;;  /
;; 1
;; This is LL heavy and rebalances with a Right Rotation
#?(:clj
   (deftest rotate-right
     (triple-node-test "ll.avl" [5 3 1])))

;; build a tree of
;;     5
;;    /
;;   1
;;    \
;;     3
;; This is LR heavy and rebalances with a Double Rotation
#?(:clj
   (deftest double-rotate-right
     (triple-node-test "lr.avl" [5 1 3])))

;; build a tree of
;; 1
;;  \
;;   3
;;    \
;;     5
;; This is RR heavy and rebalances with a Left Rotation
#?(:clj
   (deftest rotate-left
     (triple-node-test "rr.avl" [1 3 5])))

;; build a tree of
;;     1
;;      \
;;       5
;;      /
;;     3
;; This is RL heavy and rebalances with a Double Rotation
#?(:clj
   (deftest double-rotate-left
     (triple-node-test "rl.avl" [1 5 3])))


(defn five-node-left-test
  [nm vs]
  (let [empty-tree (new-block-tree create-block-manager nm long-size long-compare)
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
    (is (= (get-id f1) (get-left-id f3)))
    (is (= (get-id f5) (get-right-id f3)))

    (is (= [f3 f5] f4))

    (is (= 5 (get-data f5)))
    (is (= 0 (get-balance f5)))
    (is (= null (get-left-id f5)))
    (is (= null (get-right-id f5)))

    (is (= [f5 f7] f6))

    (is (= f7 (:root tree)))
    (is (= 7 (get-data f7)))
    (is (= left (get-balance f7)))
    (is (= (get-id f3) (get-left-id f7)))
    (is (= (get-id f9) (get-right-id f7)))

    (is (= [f7 f9] f8))
    (is (= 9 (get-data f9)))
    (is (= 0 (get-balance f9)))
    (is (= null (get-left-id f9)))
    (is (= null (get-right-id f9)))

    (is (= [f9 nil] f10))
    (close tree)))

;; build a tree of
;;       7
;;      / \
;;     5   9
;;    /
;;   3
;;  /
;; 1
;; This is LL heavy and rebalances with a Right Rotation
#?(:clj
   (deftest left-deep-rotate-right
     (five-node-left-test "ldll.avl" [7 9 5 3 1])))

;; build a tree of
;;       7
;;      / \
;;     1   9
;;      \
;;       3
;;        \
;;         5
;; This is RR heavy and rebalances with a Left Rotation
#?(:clj
   (deftest left-deep-rotate-left
     (five-node-left-test "ldrr.avl" [7 9 1 3 5])))

;; build a tree of
;;       7
;;      / \
;;     5   9
;;    /
;;   1
;;    \
;;     3
;; This is LR heavy and rebalances with a double Rotation
#?(:clj
   (deftest left-deep-double-rotate-right
     (five-node-left-test "ldlr.avl" [7 9 5 1 3])))

;; build a tree of
;;       7
;;      / \
;;     1   9
;;      \
;;       5
;;      /
;;     3
;; This is RL heavy and rebalances with a double Rotation
#?(:clj
   (deftest left-deep-double-rotate-left
     (five-node-left-test "ldrl.avl" [7 9 1 5 3])))


(defn five-node-right-test
  [nm vs]
  (let [empty-tree (new-block-tree create-block-manager nm long-size long-compare)
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
    (is (= (get-id f1) (get-left-id f3)))
    (is (= (get-id f7) (get-right-id f3)))

    (is (= [f3 f5] f4))

    (is (= 5 (get-data f5)))
    (is (= 0 (get-balance f5)))
    (is (= null (get-left-id f5)))
    (is (= null (get-right-id f5)))

    (is (= [f5 f7] f6))

    (is (= 7 (get-data f7)))
    (is (= 0 (get-balance f7)))
    (is (= (get-id f5) (get-left-id f7)))
    (is (= (get-id f9) (get-right-id f7)))

    (is (= [f7 f9] f8))
    (is (= 9 (get-data f9)))
    (is (= 0 (get-balance f9)))
    (is (= null (get-left-id f9)))
    (is (= null (get-right-id f9)))

    (is (= [f9 nil] f10))
    (close tree)))

;; build a tree of
;;       3
;;      / \
;;     1   9
;;        /
;;       7
;;      /
;;     5
;; This is LL heavy and rebalances with a Right Rotation
#?(:clj
   (deftest right-deep-rotate-right
     (five-node-right-test "rdll.avl" [3 1 9 7 5])))

;; build a tree of
;;       3
;;      / \
;;     1   5
;;          \
;;           7
;;            \
;;             9
;; This is RR heavy and rebalances with a Left Rotation
#?(:clj
   (deftest right-deep-rotate-left
     (five-node-right-test "rdrr.avl" [3 1 5 7 9])))

;; build a tree of
;;       3
;;      / \
;;     1   9
;;        /
;;       5
;;        \
;;         7
;; This is LR heavy and rebalances with a double Rotation
#?(:clj
   (deftest right-deep-double-rotate-right
     (five-node-right-test "rdlr.avl" [3 9 1 5 7])))

;; build a tree of
;;       3
;;      / \
;;     1   5
;;          \
;;           9
;;          /
;;         7
;; This is RL heavy and rebalances with a double Rotation
#?(:clj
   (deftest right-deep-double-rotate-left
     (five-node-right-test "rdrl.avl" [3 5 1 9 7])))


;; create 1024 distinct random-ish and repeatable numbers from 0 to 1023
;; builds a cycle in a mod 4096 space using 2 primes.
;; Because the low-order bits of the 2 primes are 2r01 then all numbers end in 2r01.
;; This means that the cycle is 4096/4 = 1024.
;; Truncate these bits to get our random stream.
(def pseudo-random (map #(bit-shift-right % 2) (take 1024 (iterate (fn [x] (mod (* 53 x) 4096)) 113))))

#?(:clj
   (deftest large-tree
     (let [empty-tree (new-block-tree create-block-manager "large.avl" long-size long-compare)
           tree (reduce (fn [t v]
                          (add t v long-writer))
                        empty-tree pseudo-random)
           node0 (find-node tree 0)
           root-id (get-id (:root tree))
           ;; extract the file to check that it was truncated
           block-file (:file (:block-file @(:state (:block-manager tree))))
           ro-tree (at tree root-id)]
       (is (= node0 (first-node tree)))
       (is (= (range 1024) (map get-data (node-seq tree node0))))
       (is (= (find-node tree 1023) (last-node tree)))

       (is (= (get-id (first-node tree)) (get-id (first-node ro-tree))))
       (is (= (get-id (last-node tree)) (get-id (last-node ro-tree))))

       (close tree)
       ;; check the truncated file length. Include the null node.
       (is (= (* (inc (count pseudo-random)) 3 long-size) (.length block-file)))

       (let [new-tree (new-block-tree reopen-block-manager "large.avl" long-size long-compare root-id)
             start-node (find-node new-tree 0)]
         (is (= (range (count pseudo-random)) (map get-data (node-seq new-tree start-node))))
         (close new-tree)))))

#?(:clj
   (deftest test-tx
     (let [empty-tree (new-block-tree create-block-manager "tx.avl" long-size long-compare)
           add-all (partial reduce (fn [t v] (add t v long-writer)))
           tree (add-all empty-tree (range 0 20 2)) 
           tree (commit! tree)
           node0 (find-node tree 0)
           root-id (get-id (:root tree))
           tree (add-all tree (range 1 21 2))
           node-all0 (find-node tree 0)
           node0-again (find-node (at tree root-id) 0)]
       (is (= (range 0 20 2) (map get-data (node-seq tree node0))))
       (is (= (range 20) (map get-data (node-seq tree node-all0))))
       (is (= (range 0 20 2) (map get-data (node-seq tree node0-again))))

       (close tree))))



(comment
  (defn print-structure
    "This function is only used for debugging the tree structure"
    [tree]
    (letfn [(print-node [n]
              (println ">>> ID:" (get-id n) " DATA:" (get-data n))
              (let [l (get-left-id n)
                    r (get-right-id n)]
                (println "LEFT/RIGHT: " l "/" r)
                (when-not (= null l)
                  (println "LEFT")
                  (print-node (get-child n tree left)))
                (when-not (= null r)
                  (println "RIGHT")
                  (print-node (get-child n tree right)))
                (println "----end" (get-id n))))]

      (let [r (:root tree)]
        (println "^^^^^^^^^^^^^^^^^^^^^^^")
        (println "ROOT DATA: " (get-data r))
        (println "ROOT ID: " (get-id r))
        (println)
        (print-node r)))))
