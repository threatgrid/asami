(ns ^{:doc "Tests the Data Pool"
      :author "Paula Gearon"}
    asami.durable.pool-test
  (:require [clojure.test :refer [deftest is]]
            [asami.durable.common :refer [close find-object find-id write! at]]
            [asami.durable.pool :refer [create-pool]])
  #?(:clj (:import [java.io File])))

(defn recurse-delete
  [s]
  #?(:clj
     (letfn [(remove [f]
               (when (.isDirectory f)
                 (doseq [file (into [] (.listFiles f))]
                   (remove file)))
               (.delete f))]
       (remove (File. s)))))

(deftest test-creation
  (let [pool (create-pool "empty-test")]
    (close pool)
    (recurse-delete "empty-test")))

(deftest test-encapsulate
  (let [pool (create-pool "empty-test2")
        [a pool] (write! pool "one")
        [b pool] (write! pool "two")
        [c pool] (write! pool "three")
        [d pool] (write! pool "four")
        [e pool] (write! pool "five")
        [f pool] (write! pool "six")
        [g pool] (write! pool :seven)
        [h pool] (write! pool :eight)
        [i pool] (write! pool :nine)
        [j pool] (write! pool :ten)]
    (is (= "one" (find-object pool a)))
    (is (= "two" (find-object pool b)))
    (is (= "three" (find-object pool c)))
    (is (= "four" (find-object pool d)))
    (is (= "five" (find-object pool e)))
    (is (= "six" (find-object pool f)))
    (is (= :seven (find-object pool g)))
    (is (= :eight (find-object pool h)))
    (is (= :nine (find-object pool i)))
    (is (= :ten (find-object pool j)))
    (close pool)
    (recurse-delete "empty-test2")))

(deftest test-storage
  (let [pool (create-pool "pool-test")
        data [".......one"
              ".......two"
              ".......three hundred and twenty-five"
              ".......four hundred and thirty-six"
              ".......five hundred and forty-seven"
              ".......six hundred and fifty-eight"
              :seven-hundred-and-one
              :eight-hundred-and-two
              :nine-hundred-and-three
              :ten-hundred-and-four]
        [ids pool] (reduce (fn [[ids p] d]
                             (let [[id p'] (write! p d)]
                               [(conj ids id) p']))
                           [[] pool] data)]
    (doseq [[id value] (map vector ids data)]
      (is (= value (find-object pool id))))

    (doseq [[id value] (map vector ids data)]
      (is (= id (find-id pool value)) (str "data: " value)))
    (close pool)
    (recurse-delete "pool-test")))
