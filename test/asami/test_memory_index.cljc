(ns asami.test-memory-index
  #?(:clj
      (:require [asami.index :refer [graph-add empty-graph resolve-pattern]]
                [schema.test :as st :refer [deftest]]
                [clojure.test :as t :refer [testing is run-tests]])
     :cljs
      (:require [asami.index :refer [graph-add empty-graph resolve-pattern]]
                [schema.test :as st :refer-macros [deftest]]
                [clojure.test :as t :refer-macros [testing is run-tests]])))

(t/use-fixtures :once st/validate-schemas)

(def data
  [[:a :p1 :x]
   [:a :p1 :y]
   [:a :p2 :z]
   [:a :p3 :x]
   [:b :p1 :x]
   [:b :p2 :x]
   [:b :p3 :z]
   [:c :p4 :t]])

(defn assert-data [g d]
  (reduce (partial apply graph-add) g d))

(defn unordered-resolve
  [g pattern]
  (into #{} (resolve-pattern g pattern)))

(deftest test-load
  (let [g (assert-data empty-graph data)
        r1 (unordered-resolve g '[:a ?a ?b])
        r2 (unordered-resolve g '[?a :p2 ?b])
        r3 (unordered-resolve g '[:a :p1 ?a])
        r4 (unordered-resolve g '[?a :p2 :z])
        r5 (unordered-resolve g '[:a ?a :x])
        r6 (unordered-resolve g '[:a :p4 ?a])]
    (is (= #{[:p1 :x]
             [:p1 :y]
             [:p2 :z]
             [:p3 :x]} r1))
    (is (= #{[:a :z]
             [:b :x]} r2))
    (is (= #{[:x]
             [:y]} r3))
    (is (= #{[:a]} r4))
    (is (= #{[:p1]
             [:p3]} r5))
    (is (empty? r6))))

#?(:cljs (run-tests))

