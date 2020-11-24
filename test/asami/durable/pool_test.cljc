(ns ^{:doc "Tests the Data Pool"
      :author "Paula Gearon"}
    asami.durable.pool-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.string :as s]
            [asami.durable.common :refer [close find-object find-id write! at
                                          get-object]]  ;; TODO remove
            [asami.durable.pool :refer [create-pool id-offset-long]]

            [asami.durable.tree :as tree :refer [get-child left node-seq]]
            [asami.durable.block.block-api :refer [get-long get-id get-bytes]]
            [asami.durable.encoder :refer [to-bytes]]
            [asami.durable.decoder :refer [type-info]])
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

(defn find-node
  [{index :index} s]
  (let [[header body] (to-bytes s)]
    (tree/find-node index [^byte (type-info (aget header 0)) header body s])))

(defn first-node
  [{{root :root :as index} :index}]
  (loop [n root]
    (if-let [nn (get-child n index left)]
      (recur nn)
      n)))

(deftest test-storage
  (let [pool (create-pool "pool-test")
        data ["abcdefgh"
              ".......one"
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
                           [[] pool] data)
        root (:root-id pool)
        [ids2 pool] (reduce (fn [[ids p] d]
                              (let [[id p'] (write! p d)]
                                [(conj ids id) p']))
                            [[] pool] data)]
    (is (= ids ids2))
    (is (= root (:root-id pool)))
    (doseq [[id value] (map vector ids data)]
      (is (= value (find-object pool id))))

    (doseq [[id value] (map vector ids data)]
      (is (= id (find-id pool value)) (str "data: " value)))
    (close pool)

    (let [pool2 (create-pool "pool-test" root)]
      (doseq [[id value] (map vector ids data)]
        (is (= value (find-object pool2 id))))

      (doseq [[id value] (map vector ids data)]
        (is (= id (find-id pool2 value)) (str "data: " value)))))

  (recurse-delete "pool-test"))

(deftest test-words
  (let [book (slurp "resources/pride_and_prejudice.txt")
        words (s/split book #"\s")
        pool (create-pool "book2")
        [coded bpool] (reduce (fn [[ids p] w]
                                (let [[id p'] (write! p w)]
                                  [(conj ids id) p']))
                              [[] pool]
                              words)
        root (:root-id bpool)
        g (find-id bpool "Gutenberg")
        output-words (map #(find-object bpool %) coded)]
    (is (= "Gutenberg" (find-object bpool g)))
    (is (= words output-words))

    (close bpool)

    (let [new-pool (create-pool "book2" root)
          g2 (find-id new-pool "Gutenberg")
          output-words2 (map #(find-object new-pool %) coded)]
      (is (= g g2))
      (is (= words output-words2))))
  (recurse-delete "book2"))
