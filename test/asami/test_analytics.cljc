(ns asami.test-analytics
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
  [[:a :p1 :b]
   [:a :p2 2]
   [:a :p3 :c]
   [:b :p1 :d]
   [:b :p2 3]
   [:b :p3 :e]
   [:c :p1 :f]
   [:c :p2 4]
   [:c :p3 :g]
   [:d :p1 :h]
   [:d :p2 5]
   [:e :p2 6]
   [:f :p2 7]
   [:g :p1 :i]
   [:g :p2 8]
   [:g :p3 :j]])

(def edges2
  [[:m :pr :n]
   [:o :pr :n]
   [:o :pr :p]
   [:q :pr :p]
   [:q :pr :r]
   [:s :pr :r]
   [:s :pr :t]
   [:u :pr :t]
   [:u :pr :v]])

(def edges3
  [[:x :p :y]
   [:y :p :x]])

(def edges4
  [[:z :p :z]])

(defn edges [graph] (resolve-triple graph '?s '?p '?o))

(defn node-subgraph
  [graph]
  (let [graph-1 (reduce (partial apply graph-add) graph edges1)
        subgraph-1 (subgraph-from-node graph-1 :c)
        graph-2 (reduce (partial apply graph-add) graph edges2)
        subgraph-2 (subgraph-from-node graph-2 :q)
        subgraph-2e (subgraph-from-node graph-2 :c)
        graph-3 (reduce (partial apply graph-add) graph edges3)
        subgraph-3 (subgraph-from-node graph-3 :y)
        graph-4 (reduce (partial apply graph-add) graph edges4)
        subgraph-4 (subgraph-from-node graph-4 :z)]
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
