(ns ^{:doc "Tests the LRU cache implementation"
      :author "Paula Gearon"}
    asami.durable.cache-test
  (:require [clojure.test :refer [is deftest]]
            [asami.durable.cache :refer [lookup has? hit miss evict seed
                                         lru-cache-factory]]))

(deftest populate-test
  (let [cache (-> (lru-cache-factory {1 "one"})
                  (miss 2 "two")
                  (miss 3 "three"))]
    (is (= (lookup cache 1) "one"))
    (is (= (lookup cache 2) "two"))
    (is (= (lookup cache 3) "three"))
    (is (nil? (lookup cache 4)))))

(deftest evict-test
  (let [cache (-> (lru-cache-factory {1 "one"})
                  (miss 2 "two")
                  (miss 3 "three"))]
    (is (= (lookup cache 1) "one"))
    (is (= (lookup cache 2) "two"))
    (is (= (lookup cache 3) "three"))
    (let [cache (evict cache 2)]
      (is (nil? (lookup cache 2))))))

(deftest lru-test
  (let [cache (-> (lru-cache-factory {1 "one"} :threshold 4)
                  (miss 2 "two")
                  (miss 3 "three")
                  (miss 4 "four")
                  (hit 2)
                  (hit 3)
                  (hit 4))]
    (is (= (lookup cache 1) "one"))
    (is (= (lookup cache 2) "two"))
    (is (= (lookup cache 3) "three"))
    (is (= (lookup cache 4) "four"))
    (let [cache (miss cache 5 "five")]
      (is (nil? (lookup cache 1)))
      (is (= (lookup cache 2) "two"))
      (is (= (lookup cache 3) "three"))
      (is (= (lookup cache 4) "four"))
      (is (= (lookup cache 5) "five"))))
  (let [cache (-> (lru-cache-factory {1 "one"} :threshold 4)
                  (miss 2 "two")
                  (miss 3 "three")
                  (miss 4 "four")
                  (hit 1)
                  (hit 2)
                  (hit 3)
                  (hit 2)
                  (hit 4)
                  (hit 1)
                  (hit 4)
                  (hit 4))]
    (is (= (lookup cache 1) "one"))
    (is (= (lookup cache 2) "two"))
    (is (= (lookup cache 3) "three"))
    (is (= (lookup cache 4) "four"))
    (let [cache (miss cache 5 "five")]
      (is (= (lookup cache 1) "one"))
      (is (= (lookup cache 2) "two"))
      (is (nil? (lookup cache 3)))
      (is (= (lookup cache 4) "four"))
      (is (= (lookup cache 5) "five")))))

#?(:cljs (run-tests))
