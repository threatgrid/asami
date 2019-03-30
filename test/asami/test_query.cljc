(ns asami.test-query
  "Tests internals of the query portion of the memory storage"
  (:require [asami.planner :refer [Bindings]]
            [asami.query :refer [add-to-graph pattern-left-join outer-product
                                 create-binding create-bindings]]
            [asami.graph :refer [Graph resolve-triple]]
            [asami.index :refer [empty-graph]]
            [naga.util :as u]
            [asami.core :refer [empty-store]]
            [naga.storage.store-util :refer [matching-vars]]
            [schema.core :as s]
            #?(:clj  [clojure.test :refer [is use-fixtures testing]]
               :cljs [clojure.test :refer-macros [is run-tests use-fixtures testing]])
            #?(:clj  [schema.test :as st :refer [deftest]]
               :cljs [schema.test :as st :refer-macros [deftest]]))
  #?(:clj (:import [clojure.lang ExceptionInfo])))

(use-fixtures :once st/validate-schemas)

(deftest var-mapping
  (let [m1 (matching-vars `[?a :rel ?c] `[?a ?b ?c] )
        m2 (matching-vars `[?b :rel ?f] `[?a ?b ?c ?d ?e ?f])
        m3 (matching-vars `[?b :rel ?f ?b :r2 ?e] `[?a ?b ?c ?d ?e ?f])
        m4 (matching-vars `[?x :rel ?f ?x :r2 ?e] `[?a ?b ?c ?d ?e ?f])]
    (is (= m1 {0 0, 2 2}))
    (is (= m2 {0 1, 2 5}))
    (is (= m3 {0 1, 2 5, 3 1, 5 4}))
    (is (= m4 {2 5, 5 4}))))

(def join-data
  [[:b :px :c]
   [:b :px :d]
   [:c :px :c]
   [:d :px :c]
   [:x :px :c]
   [:z :px :c]])

(deftest test-join
  (let [graph (add-to-graph empty-graph join-data)
        part-result (with-meta
                      [[:p1 :b] [:p2 :z] [:p3 :x] [:p3 :t]]
                      {:cols '[?p ?o]})
        r1 (pattern-left-join graph part-result '[?o :px :c])
        part-result2 (with-meta [[:b]] {:cols '[?o]})
        r2 (pattern-left-join graph part-result2 '[?o :px ?q])]
    (is (= '[?p ?o] (:cols (meta r1))))
    (is (= [[:p1 :b] [:p2 :z] [:p3 :x]] r1))
    (is (= '[?o ?q] (:cols (meta r2))))
    (is (= [[:b :c] [:b :d]] r2))))

(defn bnd [names vals] (with-meta vals {:cols names}))

(deftest test-outer-product
  (testing "basic"
    (let [l (bnd '[?x] [[1] [2] [3]])
          r (bnd '[?y] [[4] [5] [6]])
          p (outer-product l r)]
      (is (= [[1 4] [1 5] [1 6] [2 4] [2 5] [2 6] [3 4] [3 5] [3 6]]
             p))
      (is (= {:cols '[?x ?y]} (meta p)))))
  (testing "empty"
    (let [l (bnd '[?x] [[1] [2] [3]])
          r1 (bnd '[?y] [])
          r2 (bnd '[] [[]])
          p1 (outer-product l r1)
          p2 (outer-product l r2)]
      (is (= [] p1))
      (is (= {:cols '[?x ?y]} (meta p1)))
      (is (= [[1] [2] [3]] p2))
      (is (= {:cols '[?x]} (meta p2)))))
  (testing "single"
    (let [l (bnd '[?x] [[1] [2] [3]])
          r (bnd '[?y] [[4]])
          p (outer-product l r)]
      (is (= [[1 4] [2 4] [3 4]] p))
      (is (= {:cols '[?x ?y]} (meta p))))
    (let [l (bnd '[?x] [[1]])
          r (bnd '[?y] [[4] [5] [6]])
          p (outer-product l r)]
      (is (= [[1 4] [1 5] [1 6]] p))
      (is (= {:cols '[?x ?y]} (meta p))))
    )
  (testing "multi columns"
    (let [l (bnd '[?x ?y] [[1 2] [3 4]])
          r (bnd '[?z] [[4] [5]])
          p (outer-product l r)]
      (is (= [[1 2 4] [1 2 5] [3 4 4] [3 4 5]]
             p))
      (is (= {:cols '[?x ?y ?z]} (meta p)))))
  (testing "repeated columns"
    (let [l (bnd '[?x ?y] [[1 2] [3 4]])
          r (bnd '[?x] [[4] [5]])]
      (is (thrown? ExceptionInfo (outer-product l r))))))


(deftest test-binding
  (testing "scalar"
    (let [b (create-binding '?a 5)]
      (s/validate Bindings b)
      (is (= b [[5]]))
      (is (= '[?a] (:cols (meta b))))
      (is (thrown? ExceptionInfo (create-binding "?a" 5)))))
  (testing "tuple"
    (let [b (create-binding '[?a ?b] [1 2])]
      (s/validate Bindings b)
      (is (= b [[1 2]]))
      (is (= '[?a ?b] (:cols (meta b))))
      (is (thrown? ExceptionInfo (create-binding '[?a ?b] [1 2 3])))
      (is (thrown? ExceptionInfo (create-binding '[?a ?b] 1)))))
  (testing "collection"
    (let [b (create-binding '[?a ...] [1 2 3])]
      (s/validate Bindings b)
      (is (= b [[1] [2] [3]]))
      (is (= '[?a] (:cols (meta b))))
      (is (thrown? ExceptionInfo (create-binding '[?a ?b ...] [1 2 3])))
      (is (thrown? ExceptionInfo (create-binding '[?a ... ?b] [1 2 3])))
      (is (thrown? ExceptionInfo (create-binding '[?a ...] 1)))))
  (testing "relation"
    (let [b (create-binding '[[?a ?b]] [[1 2] [3 4] [5 6]])]
      (s/validate Bindings b)
      (is (= b [[1 2] [3 4] [5 6]]))
      (is (= '[?a ?b] (:cols (meta b))))
      (is (thrown? ExceptionInfo (create-binding '[[?a ?b] ?c] [[1 2] [3 4]])))
      (is (thrown? ExceptionInfo (create-binding '[[?a ?b] [?c ?d]] [[1 2] [3 4]])))
      (is (thrown? ExceptionInfo (create-binding '[[?a ?b ...]] [[1 2 3]])))
      (is (thrown? ExceptionInfo (create-binding '[[?a ?b "c"]] [[1 2 3]])))
      (is (thrown? ExceptionInfo (create-binding '[[?a ?b]] [[1 2] [3 4 5] [5 6]]))))))

(deftest test-bindings
  (testing "scalar"
    (let [[bds] (create-bindings '[$ ?a] [empty-store 5])
          [bds2] (create-bindings '[$ ?a ?b] [empty-store 5 6])
          [bds3] (create-bindings '[?a $] [5 empty-store])]
      (is (= [[5]] bds))
      (is (= '[?a] (:cols (meta bds))))
      (is (= [[5 6]] bds2))
      (is (= '[?a ?b] (:cols (meta bds2))))
      (is (= [[5]] bds3))
      (is (= '[?a] (:cols (meta bds3))))))
  (testing "tuple"
    (let [[bds] (create-bindings '[$ [?a ?b]] [empty-store [5 6]])
          [bds2] (create-bindings '[[?a ?b] $] [[5 6] empty-store])
          [bds3] (create-bindings '[$ [?a ?b] [?c ?d]] [empty-store [5 6] [7 8]])]
      (is (= [[5 6]] bds))
      (is (= '[?a ?b] (:cols (meta bds))))
      (is (= bds bds2))
      (is (= [[5 6 7 8]] bds3))
      (is (= '[?a ?b ?c ?d] (:cols (meta bds3))))
      (is (thrown? ExceptionInfo (create-bindings '[$ [?a ?b] [?b ?c]] [empty-store [5 6] [7 8]])))))
  (testing "collection"
    (let [[bds] (create-bindings '[$ [?a ...]] [empty-store [5 6]])
          [bds2] (create-bindings '[$ [?a ...] [?b ...]] [empty-store [5 6] [7 8]])]
      (is (= [[5] [6]] bds))
      (is (= '[?a] (:cols (meta bds))))
      (is (= [[5 7] [5 8] [6 7] [6 8]] bds2))
      (is (= '[?a ?b] (:cols (meta bds2))))))
  (testing "relation"
    (let [[bds] (create-bindings '[$ [[?a ?b]]] [empty-store [[5 6] [7 8]]])
          [bds2] (create-bindings '[$ [[?a ?b]] [[?c ?d]]] [empty-store [[5 6] [7 8]] [[1 2]]])]
      (is (= [[5 6] [7 8]] bds))
      (is (= '[?a ?b] (:cols (meta bds))))
      (is (= [[5 6 1 2] [7 8 1 2]] bds2))
      (is (= '[?a ?b ?c ?d] (:cols (meta bds2))))))
  (testing "mix"
    (let [[bds] (create-bindings '[?x [?a ...] [?b ?c] [[?d ?e]]] [5 [11 12] [2 3] [[:a :b] [:c :d]]])]
      (is (= [[5 11 2 3 :a :b] [5 11 2 3 :c :d] [5 12 2 3 :a :b] [5 12 2 3 :c :d]]))
      (is (= '[?x ?a ?b ?c ?d ?e])))))

#?(:clj
   (deftest test-eval
     (is (= 5 (u/c-eval 5)))
     (is ((u/c-eval '(fn [[?a ?b]] (= ?b :z))) [0 :z]))))

#?(:cljs (run-tests))
