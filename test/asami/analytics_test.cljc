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
  [[:mem/a :p1 :mem/b]
   [:mem/a :p2 2]
   [:mem/a :p3 :mem/c]
   [:mem/b :p1 :mem/d]
   [:mem/b :p2 3]
   [:mem/b :p3 :mem/e]
   [:mem/c :p1 :mem/f]
   [:mem/c :p2 4]
   [:mem/c :p3 :mem/g]
   [:mem/d :p1 :mem/h]
   [:mem/d :p2 5]
   [:mem/e :p2 6]
   [:mem/f :p2 7]
   [:mem/g :p1 :mem/i]
   [:mem/g :p2 8]
   [:mem/g :p3 :mem/j]])

(def edges2
  [[:mem/m :pr :mem/n]
   [:mem/o :pr :mem/n]
   [:mem/o :pr :mem/p]
   [:mem/q :pr :mem/p]
   [:mem/q :pr :mem/r]
   [:mem/s :pr :mem/r]
   [:mem/s :pr :mem/t]
   [:mem/u :pr :mem/t]
   [:mem/u :pr :mem/v]])

(def edges3
  [[:mem/x :p :mem/y]
   [:mem/y :p :mem/x]])

(def edges4
  [[:mem/z :p :mem/z]])

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
