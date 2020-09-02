(ns asami.test-query-internals
  "Tests internals of the query portion of the memory storage"
  (:require [asami.planner :refer [Bindings]]
            [asami.core :as core]
            [asami.query :as q :refer [pattern-left-join outer-product
                                       create-binding create-bindings minus left-join disjunction
                                       result-label aggregate-over aggregate-query]]
            [asami.graph :refer [Graph resolve-triple graph-transact]]
            [asami.index :refer [empty-graph]]
            [asami.internal :as internal]
            [zuko.util :as u]
            [zuko.schema :refer [vartest?]]
            [naga.storage.store-util :refer [matching-vars project]]
            [schema.core :as s]
            #?(:clj  [clojure.test :refer [is use-fixtures testing]]
               :cljs [clojure.test :refer-macros [is run-tests use-fixtures testing]])
            #?(:clj  [schema.test :as st :refer [deftest]]
               :cljs [schema.test :as st :refer-macros [deftest]]))
  #?(:clj (:import [clojure.lang ExceptionInfo])))

(use-fixtures :once st/validate-schemas)

(defn assert-data
  [graph data]
  (graph-transact graph 0 data nil))

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
  (let [graph (assert-data empty-graph join-data)
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
    (let [[bds] (create-bindings '[$ ?a] [empty-graph 5])
          [bds2] (create-bindings '[$ ?a ?b] [empty-graph 5 6])
          [bds3] (create-bindings '[?a $] [5 empty-graph])]
      (is (= [[5]] bds))
      (is (= '[?a] (:cols (meta bds))))
      (is (= [[5 6]] bds2))
      (is (= '[?a ?b] (:cols (meta bds2))))
      (is (= [[5]] bds3))
      (is (= '[?a] (:cols (meta bds3))))))
  (testing "tuple"
    (let [[bds] (create-bindings '[$ [?a ?b]] [empty-graph [5 6]])
          [bds2] (create-bindings '[[?a ?b] $] [[5 6] empty-graph])
          [bds3] (create-bindings '[$ [?a ?b] [?c ?d]] [empty-graph [5 6] [7 8]])]
      (is (= [[5 6]] bds))
      (is (= '[?a ?b] (:cols (meta bds))))
      (is (= bds bds2))
      (is (= [[5 6 7 8]] bds3))
      (is (= '[?a ?b ?c ?d] (:cols (meta bds3))))
      (is (thrown? ExceptionInfo (create-bindings '[$ [?a ?b] [?b ?c]] [empty-graph [5 6] [7 8]])))))
  (testing "collection"
    (let [[bds] (create-bindings '[$ [?a ...]] [empty-graph [5 6]])
          [bds2] (create-bindings '[$ [?a ...] [?b ...]] [empty-graph [5 6] [7 8]])]
      (is (= [[5] [6]] bds))
      (is (= '[?a] (:cols (meta bds))))
      (is (= [[5 7] [5 8] [6 7] [6 8]] bds2))
      (is (= '[?a ?b] (:cols (meta bds2))))))
  (testing "relation"
    (let [[bds] (create-bindings '[$ [[?a ?b]]] [empty-graph [[5 6] [7 8]]])
          [bds2] (create-bindings '[$ [[?a ?b]] [[?c ?d]]] [empty-graph [[5 6] [7 8]] [[1 2]]])]
      (is (= [[5 6] [7 8]] bds))
      (is (= '[?a ?b] (:cols (meta bds))))
      (is (= [[5 6 1 2] [7 8 1 2]] bds2))
      (is (= '[?a ?b ?c ?d] (:cols (meta bds2))))))
  (testing "mix"
    (let [[bds] (create-bindings '[?x [?a ...] [?b ?c] [[?d ?e]]] [5 [11 12] [2 3] [[:a :b] [:c :d]]])]
      (is (= [[5 11 2 3 :a :b] [5 11 2 3 :c :d] [5 12 2 3 :a :b] [5 12 2 3 :c :d]]))
      (is (= '[?x ?a ?b ?c ?d ?e])))))

(def minus-data
  [[:b :px :c]
   [:b :px :d]
   [:c :py :c]
   [:d :px :c]
   [:x :py :c]
   [:z :px :c]])

(deftest test-minus
  (let [graph (assert-data empty-graph minus-data)
        part-result (with-meta
                      [[:p1 :a] [:p1 :b] [:p2 :z] [:p3 :x] [:p3 :t]]
                      {:cols '[?p ?o]})
        r1 (minus graph part-result '(not [?o :px :c]))
        part-result2 (with-meta [[:b] [:c] [:d] [:x]] {:cols '[?o]})
        r2 (minus graph part-result2 '(not [?o :px ?q] [?q :px ?r]))
        r2' (left-join '(not [?o :px ?q] [?q :px ?r]) part-result2 graph)]
    (is (= '[?p ?o] (:cols (meta r1))))
    (is (= [[:p1 :a] [:p3 :x] [:p3 :t]] r1))
    (is (= '[?o] (:cols (meta r2))))
    (is (= [[:c] [:d] [:x]] r2))
    (is (= [[:c] [:d] [:x]] r2'))))

(def or-data
  [[:a :px :m]
   [:a :py :n]
   [:b :px :o]
   [:c :py :p]
   [:d :pz :q]])

(deftest test-or
  (let [graph (assert-data empty-graph or-data)
        part-result (with-meta
                      [[:a] [:b] [:c] [:d]]
                      {:cols '[?e]})
        r1 (disjunction graph part-result '(or [?e :px ?v] [?e :py ?v]))]
    (is (= '[?e ?v] (:cols (meta r1))))
    (is (= [[:a :m] [:b :o] [:a :n] [:c :p]] r1))
    (is (thrown-with-msg?
         ExceptionInfo
         #"Alternate sides of OR clauses may not contain different vars"
         (disjunction graph part-result '(or [?e :px ?v] [?e :py ?w]))))))


(deftest test-result-label
  "Tests the result-label function, which renames aggregates to addressable labels"
  (is (= '?a (result-label '?a)))
  (is (= '?count-a (result-label '(count ?a))))
  (is (thrown? ExceptionInfo (result-label '[?a])))
  (is (thrown? ExceptionInfo (result-label '[count ?a]))))

(deftest test-aggregate-over
  "Tests the aggregation/projection operation"
  (let [columns {:cols '[?a ?b ?c]}
        data1 [[:a :b :c]
               [:a :b :d]
               [:a :b :e]
               [:a :b :f]
               [:a :b :g]]
        data2 [[:a :m :c]
               [:a :m :d]]
        data3 [[:t :u :x]
               [:t :u :y]
               [:t :u :z]]
        unaggregated [(with-meta data1 columns)
                      (with-meta data2 columns)
                      (with-meta data3 columns)]
        results1 (aggregate-over '[?a ?b (count ?c)] unaggregated)
        results2 (aggregate-over '[?a ?b (first ?c)] unaggregated)
        results3 (aggregate-over '[?b ?a (last ?c)] unaggregated)]
    (is (= '[?a ?b ?count-c] (:cols (meta results1))))
    (is (= '[[:a :b 5] [:a :m 2] [:t :u 3]] results1))
    (is (= '[?a ?b ?first-c] (:cols (meta results2))))
    (is (= '[[:a :b :c] [:a :m :c] [:t :u :x]] results2))
    (is (= '[?b ?a ?last-c] (:cols (meta results3))))
    (is (= '[[:b :a :g] [:m :a :d] [:u :t :z]] results3)))

  (let [columns {:cols '[?a ?c]}
        data1 [[:a 1]
               [:a 2]
               [:a 3]
               [:a 4]
               [:a 5]]
        data2 [[:a 6]
               [:a 8]]
        data3 [[:t 10]
               [:t 11]
               [:t 12]]
        unaggregated [(with-meta data1 columns)
                      (with-meta data2 columns)
                      (with-meta data3 columns)]
        results1 (aggregate-over '[(count ?c)] unaggregated)
        results2 (aggregate-over '[?a (sum ?c)] unaggregated)
        results3 (aggregate-over '[(avg ?c) ?a] unaggregated)
        results4 (aggregate-over '[?a (max ?c)] unaggregated)
        results5 (aggregate-over '[?a (min ?c)] unaggregated)]
    (is (= '[?count-c] (:cols (meta results1))))
    (is (= '[[5] [2] [3]] results1))
    (is (= '[?a ?sum-c] (:cols (meta results2))))
    (is (= '[[:a 15] [:a 14] [:t 33]] results2))
    (is (= '[?avg-c ?a] (:cols (meta results3))))
    (is (= '[[3 :a] [7 :a] [11 :t]] results3))
    (is (= '[?a ?max-c] (:cols (meta results4))))
    (is (= '[[:a 5] [:a 8] [:t 12]] results4))
    (is (= '[?a ?min-c] (:cols (meta results5))))
    (is (= '[[:a 1] [:a 6] [:t 10]] results5))))


(let [agg-data [[:a :p "first"]
                [:a :p2 :one]
                [:a :p2 :two]
                [:a :p2 :three]
                [:b :p "second"]
                [:b :p2 :b-one]
                [:b :p2 :b-two]
                [:b :p2 :b-three]
                [:b :p2 :b-four]
                [:b :p2 :b-five]]]
  

  (deftest test-aggregate-query
    "Tests the function that does query aggregation.
   This function does lots of work after the arguments have been extracted"
    (let [find '[?a ?b (count ?c)]
          bindings q/empty-bindings
          with []
          where '[[?a :p ?b] [?a :p2 ?c]]
          graph (assert-data empty-graph agg-data)
          project-fn (partial project internal/project-args)

          r (aggregate-query find bindings with where graph project-fn {})]
      (is (= '[?a ?b ?count-c] (:cols (meta r))))
      (is (= [[:a "first" 3] [:b "second" 5]] r)))

    (let [find '[(count ?c)]
          bindings q/empty-bindings
          with []
          where '[[?a :p ?c]]
          graph (assert-data empty-graph agg-data)
          project-fn (partial project internal/project-args)

          r (aggregate-query find bindings with where graph project-fn {})]
      (is (= '[?count-c] (:cols (meta r))))
      (is (= [[2]] r)))))

#?(:clj
   (deftest test-eval
     (is (= 5 (u/c-eval 5)))
     (is ((u/c-eval '(fn [[?a ?b]] (= ?b :z))) [0 :z]))))

(deftest test-fn-for
  (is ((u/fn-for '=) 5 5))
  (is (= ((u/fn-for 'str) "a" 5) "a5")))

(deftest test-query-parsing
  (testing "wildcards are replaced with fresh variables"
    (let [query (q/parse '{:find [?v], :where [[_ _ ?v]]})
          [[e a v]] (get query :where)]
      (is (vartest? e))
      (is (vartest? a))
      (is (= '?v v))))

  (testing "Shortened pattern constraints are filled in"
    (let [query (q/parse '{:find [?e ?v]
                           :where [[?e]
                                   [?e :p]
                                   [?e :p ?v]]})
          [[e1 a1 v1]
           [e2 a2 v2]
           [e3 a3 v3]] (get query :where)]
      (is (= '?e e1))
      (is (vartest? a1))
      (is (vartest? v1))

      (is (= '?e e2))
      (is (= :p a2))
      (is (vartest? v2))

      (is (= '?e e3))
      (is (= :p a3))
      (is (= '?v v3)))

    (let [query (q/parse '{:find [?e]
                           :where [(not [?e])]})
          [[op [e p v]]] (get query :where)]
      (is (= 'not op))
      (is (= '?e e))
      (is (vartest? p))
      (is (vartest? v)))))

#?(:cljs (run-tests))
