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
         (put-long! block (+ tuple-offset offset) (nth tuple offset)))
       block))
   block
   tuples))

(deftest tuple-block-search
  (let [block (new-block (* block-max tuple-size long-size))
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
