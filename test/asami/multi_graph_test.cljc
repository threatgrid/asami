(ns asami.multi-graph-test
  #?(:clj
     (:require [asami.graph :refer [resolve-pattern count-pattern graph-delete]]
               [asami.multi-graph :refer [empty-multi-graph multi-graph-add]]
               [schema.test :as st :refer [deftest]]
               [clojure.test :as t :refer [testing is run-tests]]
               [clojure.pprint :refer [pprint]])
     :cljs
     (:require [asami.graph :refer [resolve-pattern count-pattern graph-delete]]
               [asami.multi-graph :refer [empty-multi-graph multi-graph-add]]
               [schema.test :as st :refer-macros [deftest]]
               [clojure.test :as t :refer-macros [testing is run-tests]])))

(t/use-fixtures :once st/validate-schemas)

(def data
  [[:a :p1 :x]
   [:a :p1 :x]
   [:a :p1 :y]
   [:a :p2 :z]
   [:a :p2 :z]
   [:a :p2 :z]
   [:a :p3 :x]
   [:b :p1 :x]
   [:b :p1 :x]
   [:b :p2 :x]
   [:b :p2 :x]
   [:b :p3 :z]
   [:c :p4 :t]])

(def data2
  [[[:a :p1 :x] 2]
   [[:a :p1 :y] 1]
   [[:a :p2 :z] 3]
   [[:a :p3 :x] 1]
   [[:b :p1 :x] 2]
   [[:b :p2 :x] 2]
   [[:b :p3 :z] 1]
   [[:c :p4 :t] 1]])

(defn assert-data
  ([g d]
   (assert-data g d 1))
  ([g d tx]
   (reduce (fn [g [s p o]] (multi-graph-add g s p o tx)) g d)))

(defn sort-resolve
  [g pattern]
  (sort (resolve-pattern g pattern)))

(deftest test-load
  (let [g (assert-data empty-multi-graph data)
        r1 (sort-resolve g '[:a ?a ?b])
        r2 (sort-resolve g '[?a :p2 ?b])
        r3 (sort-resolve g '[:a :p1 ?a])
        r4 (sort-resolve g '[?a :p2 :z])
        r5 (sort-resolve g '[:a ?a :x])
        r6 (sort-resolve g '[:a :p4 ?a])
        r6' (sort-resolve g '[:a :p3 ?a])
        r7 (sort-resolve g '[:a :p1 :x])
        r8 (sort-resolve g '[:a :p1 :b])
        r9 (sort-resolve g '[?a ?b ?c])]
    (is (= [[:p1 :x]
            [:p1 :x]
            [:p1 :y]
            [:p2 :z]
            [:p2 :z]
            [:p2 :z]
            [:p3 :x]] r1))
    (is (= [[:a :z]
            [:a :z]
            [:a :z]
            [:b :x]
            [:b :x]] r2))
    (is (= [[:x]
            [:x]
            [:y]] r3))
    (is (= [[:a] [:a] [:a]] r4))
    (is (= [[:p1]
            [:p1]
            [:p3]] r5))
    (is (empty? r6))
    (is (= [[:x]] r6'))
    (is (= [[] []] r7))
    (is (empty? r8))
    (is (= data r9))))

(deftest test-count
  (let [g (assert-data empty-multi-graph data)
        r1 (count-pattern g '[:a ?a ?b])
        r2 (count-pattern g '[?a :p2 ?b])
        r3 (count-pattern g '[:a :p1 ?a])
        r4 (count-pattern g '[?a :p2 :z])
        r5 (count-pattern g '[:a ?a :x])
        r6 (count-pattern g '[:a :p4 ?a])
        r6' (count-pattern g '[:a :p3 ?a])
        r7 (count-pattern g '[:a :p1 :x])
        r8 (count-pattern g '[:a :p1 :b])
        r9 (count-pattern g '[?a ?b ?c])]
    (is (= 4 r1))
    (is (= 2 r2))
    (is (= 2 r3))
    (is (= 1 r4))
    (is (= 2 r5))
    (is (zero? r6))
    (is (= 1 r6'))
    (is (= 1 r7))
    (is (zero? r8))
    (is (= 8 r9))))

(deftest test-load2
  (let [g (reduce (fn [g [[s p o] n]] (multi-graph-add g s p o 1 n)) empty-multi-graph data2)
        r1 (sort-resolve g '[:a ?a ?b])
        r2 (sort-resolve g '[?a :p2 ?b])
        r3 (sort-resolve g '[:a :p1 ?a])
        r4 (sort-resolve g '[?a :p2 :z])
        r5 (sort-resolve g '[:a ?a :x])
        r6 (sort-resolve g '[:a :p4 ?a])
        r6' (sort-resolve g '[:a :p3 ?a])
        r7 (sort-resolve g '[:a :p1 :x])
        r8 (sort-resolve g '[:a :p1 :b])
        r9 (sort-resolve g '[?a ?b ?c])]
    (is (= [[:p1 :x]
            [:p1 :x]
            [:p1 :y]
            [:p2 :z]
            [:p2 :z]
            [:p2 :z]
            [:p3 :x]] r1))
    (is (= [[:a :z]
            [:a :z]
            [:a :z]
            [:b :x]
            [:b :x]] r2))
    (is (= [[:x]
            [:x]
            [:y]] r3))
    (is (= [[:a] [:a] [:a]] r4))
    (is (= [[:p1]
            [:p1]
            [:p3]] r5))
    (is (empty? r6))
    (is (= [[:x]] r6'))
    (is (= [[] []] r7))
    (is (empty? r8))
    (is (= data r9))))


(deftest test-remove
  (let [g (assert-data empty-multi-graph data)
        g2 (reduce (fn [g [s p o]] (graph-delete g s p o)) g data)
        r1 (sort-resolve g2 '[:a ?a ?b])
        r2 (sort-resolve g2 '[?a :p2 ?b])
        r3 (sort-resolve g2 '[:a :p1 ?a])
        r4 (sort-resolve g2 '[?a :p2 :z])
        r5 (sort-resolve g2 '[:a ?a :x])
        r6 (sort-resolve g2 '[:a :p4 ?a])
        r6' (sort-resolve g2 '[:a :p3 ?a])
        r7 (sort-resolve g2 '[:a :p1 :x])
        r8 (sort-resolve g2 '[:a :p1 :b])
        r9 (sort-resolve g2 '[?a ?b ?c])]
    (is (= [] r1))
    (is (= [] r2))
    (is (= [] r3))
    (is (= [] r4))
    (is (= [] r5))
    (is (empty? r6))
    (is (= [] r6'))
    (is (= [] r7))
    (is (empty? r8))
    (is (empty? r9))))

(deftest test-txn-values
  (let [g (-> empty-multi-graph
              (assert-data (take 2 data) 1)
              (assert-data (drop 2 data) 2))
        last-stmt-id (count data)]
    (is (= (inc last-stmt-id) (:next-stmt-id g)))
    ;; TODO use actual queries instead once they're supported
    (is (= 1 (get-in g [:spo :a :p1 :x :t])))
    (is (= 1 (get-in g [:osp :x :a :p1 :t])))
    (is (= 1 (get-in g [:pos :p1 :x :a :t])))

    (is (= 2 (get-in g [:spo :c :p4 :t :t])))
    (is (= 2 (get-in g [:osp :t :c :p4 :t])))
    (is (= 2 (get-in g [:pos :p4 :t :c :t])))

    (is (= 1 (get-in g [:spo :a :p1 :x :id])))
    (is (= 1 (get-in g [:osp :x :a :p1 :id])))
    (is (= 1 (get-in g [:pos :p1 :x :a :id])))

    (is (= last-stmt-id (get-in g [:spo :c :p4 :t :id])))
    (is (= last-stmt-id (get-in g [:osp :t :c :p4 :id])))
    (is (= last-stmt-id (get-in g [:pos :p4 :t :c :id])))))

#?(:cljs (run-tests))

