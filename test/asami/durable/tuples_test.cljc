(ns ^{:doc "Tests for the tuples store"
      :author "Paula Gearon"}
    asami.durable.tuples-test
  (:require [clojure.test :refer [deftest is]]
            [asami.durable.test-utils :refer [new-block]]
            [asami.durable.common :refer [long-size]]
            [asami.durable.tuples :refer [search-block tuple-size block-max]]
            [asami.durable.block.block-api :refer [get-long put-long!]]))

(defn add-tuples
  [block tuples]
  (reduce-kv
   (fn [b n tuple]
     (let [tuple-offset (* n tuple-size)]
       (doseq [offset (range tuple-size)]
         (put-long! block (+ tuple-offset offset) (nth tuple offset)))))
   block
   tuples))

(deftest tuple-block-search
  (let [block (new-block (* block-max tuple-size long-size))
        block (add-tuples block
                          [[1 2 3]
                           [1 2 5]
                           [1 5 3]
                           [1 6 2]
                           [10 2 3]
                           [10 2 4]
                           [10 5 7]
                           [10 6 2]
                           [15 2 4]
                           [16 2 3]
                           [16 6 2]])]
    (is (= 3 (search-block block [1 6 2])))
    (is (= 4 (search-block block [10 2 3])))
    (is (= 0 (search-block block [1 2 3])))
    (is (= 11 (search-block block [16 6 2])))

    (is (= [0 1] (search-block block [1 2 4])))
    (is (= [1 2] (search-block block [1 3 40])))
    (is (= [3 4] (search-block block [4 1 1])))

    ;; testing edge case. This is not called
    (is (= [0 1] (search-block block [1 1 1])))
    (is (= [10 11] (search-block block [16 7 0])))))
