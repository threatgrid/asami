(ns asami.analytics-test
  "Tests analytics functions"
  (:require
            [asami.graph :refer [Graph graph-add resolve-triple]]
            [asami.index :refer [empty-graph]]
            [asami.multi-graph :refer [empty-multi-graph]]
            [asami.analytics :refer [subgraph-from-node subgraph-entities subgraphs]]
            #?(:clj  [schema.core :as s]
               :cljs [schema.core :as s :include-macros true])
            #?(:clj  [clojure.test :refer [is use-fixtures testing]]
               :cljs [clojure.test :refer-macros [is run-tests use-fixtures testing]])
            #?(:clj  [schema.test :as st :refer [deftest]]
               :cljs [schema.test :as st :refer-macros [deftest]]))
  #?(:clj (:import [clojure.lang ExceptionInfo])))

(use-fixtures :once st/validate-schemas)

(def edges1
  [[:mem/a :p1 :mem/b 1]
   [:mem/a :p2 2 1]
   [:mem/a :p3 :mem/c 1]
   [:mem/b :p1 :mem/d 1]
   [:mem/b :p2 3 1]
   [:mem/b :p3 :mem/e 1]
   [:mem/c :p1 :mem/f 1]
   [:mem/c :p2 4 1]
   [:mem/c :p3 :mem/g 1]
   [:mem/d :p1 :mem/h 1]
   [:mem/d :p2 5 1]
   [:mem/e :p2 6 1]
   [:mem/f :p2 7 1]
   [:mem/g :p1 :mem/i 1]
   [:mem/g :p2 8 1]
   [:mem/g :p3 :mem/j 1]])

(def edges2
  [[:mem/m :pr :mem/n 2]
   [:mem/o :pr :mem/n 2]
   [:mem/o :pr :mem/p 2]
   [:mem/q :pr :mem/p 2]
   [:mem/q :pr :mem/r 2]
   [:mem/s :pr :mem/r 2]
   [:mem/s :pr :mem/t 2]
   [:mem/u :pr :mem/t 2]
   [:mem/u :pr :mem/v 2]])

(def edges3
  [[:mem/x :p :mem/y 3]
   [:mem/y :p :mem/x 3]])

(def edges4
  [[:mem/z :p :mem/z 4]])

(defn edges [graph] (resolve-triple graph '?s '?p '?o))

(defn node-subgraph
  [graph]
  (let [graph-1 (reduce (partial apply graph-add) graph edges1)
        subgraph-1 (subgraph-from-node graph-1 :mem/c)
        graph-2 (reduce (partial apply graph-add) graph edges2)
        subgraph-2 (subgraph-from-node graph-2 :mem/q)
        subgraph-2e (subgraph-from-node graph-2 :mem/c)
        graph-3 (reduce (partial apply graph-add) graph edges3)
        subgraph-3 (subgraph-from-node graph-3 :mem/y)
        graph-4 (reduce (partial apply graph-add) graph edges4)
        subgraph-4 (subgraph-from-node graph-4 :mem/z)]
    (is (= 7 (count subgraph-1)))
    (is (= 5 (count subgraph-2)))
    (is (empty? subgraph-2e))
    (is (= 2 (count subgraph-3)))
    (is (= 1 (count subgraph-4)))))

(deftest test-node-subgraph
  (node-subgraph empty-graph)
  (node-subgraph empty-multi-graph))

(defn edge-count [g] (count (edges g)))

(defn all-subgraphs
  [graph]
  (let [subgraphs-0 (subgraphs graph)
        graph-1 (reduce (partial apply graph-add) graph edges1)
        subgraphs-1 (subgraphs graph-1)
        graph-2 (reduce (partial apply graph-add) graph-1 edges2)
        subgraphs-2 (subgraphs graph-2)
        graph-3 (reduce (partial apply graph-add) graph-2 edges3)
        subgraphs-3 (subgraphs graph-3)
        graph-4 (reduce (partial apply graph-add) graph-3 edges4)
        subgraphs-4 (subgraphs graph-4)]
    (is (empty? subgraphs-0))
    (is (= 1 (count subgraphs-1)))
    (is (= #{(count edges1)}
           (set (map edge-count subgraphs-1))))
    (is (= 2 (count subgraphs-2)))
    (is (= #{(count edges1) (count edges2)}
           (set (map edge-count subgraphs-2))))
    (is (= 3 (count subgraphs-3)))
    (is (= #{(count edges1) (count edges2) (count edges3)}
           (set (map edge-count subgraphs-3))))
    (is (= 4 (count subgraphs-4)))
    (is (= #{(count edges1) (count edges2) (count edges3) (count edges4)}
           (set (map edge-count subgraphs-4))))))

(deftest test-subgraphs
  (all-subgraphs empty-graph)
  (all-subgraphs empty-multi-graph))

#?(:cljs (run-tests))
