(ns asami.test-planner
  "Tests internals of the query portion of the memory storage"
  (:require [asami.planner :refer [bindings-chain first-group min-join-path
                                   plan-path merge-operations Bindings]]
            [asami.query]  ;; required, as this defines the HasVars protocol for objects
            [asami.graph :refer [Graph resolve-triple]]
            [naga.storage.store-util :refer [matching-vars]]
            [schema.core :as s]
            #?(:clj  [clojure.test :refer [is use-fixtures testing]]
               :cljs [clojure.test :refer-macros [is run-tests use-fixtures testing]])
            #?(:clj  [schema.test :as st :refer [deftest]]
               :cljs [schema.test :as st :refer-macros [deftest]]))
  #?(:clj (:import [clojure.lang ExceptionInfo])))

(use-fixtures :once st/validate-schemas)

(deftest test-query-path
  (let [simple-p '[[?a :a :b] [?b :c :d]]
        simple-cm '{[?a :a :b] 1, [?b :c :d] 1}
        [g] (first-group nil simple-p [])
        [g*] (first-group #{'?b} simple-p [])
        p (min-join-path nil simple-cm simple-p [])
        simple-p2 '[[?a :a :b] [?b :c :d] [?c :e ?b] [?a :c :d]]
        simple-cm2 '{[?a :a :b] 1, [?b :c :d] 2, [?c :e ?b] 1, [?a :c :d] 1}
        [g2] (first-group nil simple-p2 [])
        [g2*] (first-group #{'?a} simple-p2 [])
        [g2**] (first-group #{'?c} simple-p2 [])
        p2 (min-join-path nil simple-cm2 simple-p2 [])
        patterns '[[?a :a :b]
                   [?b :c ?d]
                   [?d :d ?e]
                   [?d :e ?f]
                   [?f :f ?a]
                   [?f :g ?g]
                   [?g :v1 ?v1]
                   [?g :v2 ?v2]
                   [?h :v1 ?v1]
                   [?h :v2 ?v2]
                   [?i :i ?h]
                   [?other :id "id"]]
        count-map '{[?a :a :b] 1
                    [?b :c ?d] 2
                    [?d :d ?e] 3
                    [?d :e ?f] 3
                    [?f :f ?a] 3
                    [?f :g ?g] 5
                    [?g :v1 ?v1] 3
                    [?g :v2 ?v2] 4
                    [?h :v1 ?v1] 5
                    [?h :v2 ?v2] 6
                    [?i :i ?h] 7
                    [?other :id "id"] 1}
        [group] (first-group nil patterns [])
        path (min-join-path nil count-map patterns [])]

    (is (= '[[?a :a :b]] g))
    (is (= '[[?b :c :d]] g*))
    (is (= '[[?a :a :b] [?b :c :d]] p))

    (is (= '[[?a :a :b] [?a :c :d]] g2))
    (is (= '[[?a :a :b] [?a :c :d]] g2*))
    (is (= '[[?c :e ?b] [?b :c :d]] g2**))
    (is (= '[[?a :a :b] [?a :c :d] [?c :e ?b] [?b :c :d]] p2))

    (is (= '[[?a :a :b]
             [?f :f ?a]
             [?f :g ?g]
             [?g :v1 ?v1]
             [?g :v2 ?v2]
             [?h :v1 ?v1]
             [?h :v2 ?v2]
             [?i :i ?h]
             [?d :e ?f]
             [?b :c ?d]
             [?d :d ?e]] group))
    (is (= '[[?a :a :b]
             [?f :f ?a]
             [?d :e ?f]
             [?b :c ?d]
             [?d :d ?e]
             [?f :g ?g]
             [?g :v1 ?v1]
             [?g :v2 ?v2]
             [?h :v1 ?v1]
             [?h :v2 ?v2]
             [?i :i ?h]
             [?other :id "id"]] path))))


(defn mapto [s1 s2]
  (into {} (filter second (map vector s1 s2))))

(deftest test-query-paths
  (let [short-patterns '[[?a :b ?c]
                         [?d :e :f]
                         [?c ?d ?e]]
        path1 (min-join-path nil (mapto short-patterns [1 2 3])
                             short-patterns [])
        path2 (min-join-path nil (mapto short-patterns [2 1 3])
                             short-patterns [])
        path3 (min-join-path nil (mapto short-patterns [2 3 1])
                             short-patterns [])
        path4 (min-join-path nil (mapto short-patterns [3 2 1])
                             short-patterns [])]
    (is (= '[[?a :b ?c]
             [?c ?d ?e]
             [?d :e :f]]
           path1))
    (is (= '[[?d :e :f]
             [?c ?d ?e]
             [?a :b ?c]]
           path2))
    (is (= '[[?c ?d ?e]
             [?a :b ?c]
             [?d :e :f]]
           path3))
    (is (= '[[?c ?d ?e]
             [?d :e :f]
             [?a :b ?c]]
           path4))))


(defrecord StubResolver [counts]
  Graph
  (graph-add [this _ _ _] this)
  (graph-delete [this _ _ _] this)
  (resolve-triple [store s p o] (repeat (get counts [s p o])
                                        [:s :p :o]))
  (count-triple [store s p o] (count (resolve-triple store s p o))))

(defn resolver-for [patterns counts]
  (let [m (mapto patterns counts)]
    (->StubResolver m)))

(deftest test-filtered-query-paths
  (let [short-patterns [(with-meta '[(= ?d 5)] {:vars '#{?d}})
                        '[?a :b ?c]
                        '[?d :e :f]
                        (with-meta '[(not= ?e ?a)] {:vars '#{?e ?a}})
                        '[?c ?d ?e]]
        path1 (plan-path (resolver-for short-patterns [nil 1 2 nil 3])
                          short-patterns {})
        path2 (plan-path (resolver-for short-patterns [nil 2 1 nil 3])
                            short-patterns {})
        path3 (plan-path (resolver-for short-patterns [nil 2 3 nil 1])
                          short-patterns {})
        path4 (plan-path (resolver-for short-patterns [nil 3 2 nil 1])
                          short-patterns {})]
    (is (= '[[?a :b ?c]
             [?c ?d ?e]
             [(= ?d 5)]
             [(not= ?e ?a)]
             [?d :e :f]]
           path1))
    (is (= '[[?d :e :f]
             [(= ?d 5)]
             [?c ?d ?e]
             [?a :b ?c]
             [(not= ?e ?a)]]
           path2))
    (is (= '[[?c ?d ?e]
             [(= ?d 5)]
             [?a :b ?c]
             [(not= ?e ?a)]
             [?d :e :f]]
           path3))
    (is (= '[[?c ?d ?e]
             [(= ?d 5)]
             [?d :e :f]
             [?a :b ?c]
             [(not= ?e ?a)]]
           path4))))

(deftest var-mapping
  (let [m1 (matching-vars `[?a :rel ?c] `[?a ?b ?c] )
        m2 (matching-vars `[?b :rel ?f] `[?a ?b ?c ?d ?e ?f])
        m3 (matching-vars `[?b :rel ?f ?b :r2 ?e] `[?a ?b ?c ?d ?e ?f])
        m4 (matching-vars `[?x :rel ?f ?x :r2 ?e] `[?a ?b ?c ?d ?e ?f])]
    (is (= m1 {0 0, 2 2}))
    (is (= m2 {0 1, 2 5}))
    (is (= m3 {0 1, 2 5, 3 1, 5 4}))
    (is (= m4 {2 5, 5 4}))))

(deftest test-merge-filters
  (is (= '[[:a ?a ?b] [(= ?b :z)]] (merge-operations nil {} '[[:a ?a ?b]] [] '[[(= ?b :z)]] [])))
  (is (= '[[:x ?c ?a] [:a ?a ?b] [(= ?b :z)]]
         (merge-operations nil {} '[[:x ?c ?a] [:a ?a ?b]] [] '[[(= ?b :z)]] [])))
  (is (= '[[:x ?c ?a] [(= ?a :z)] [:a ?a ?b]]
         (merge-operations nil {} '[[:x ?c ?a] [:a ?a ?b]] [] '[[(= ?a :z)]] []))))

(deftest test-bindings-chain
  (let [[ins1 outs1] (bindings-chain '[[(inc ?c) ?d] [(str ?b "-" ?d) ?e] [(dec ?c) ?f]]
                                     '#{?a ?b ?c}
                                     '[[?x :label ?e] [?y :included true]])
        [ins2 outs2] (bindings-chain '[[(inc ?c) ?d] [(str ?b "-" ?d) ?e] [(dec ?c) ?f]]
                                     '#{?a ?b}
                                     '[[?x :label ?e] [?y :included true]])
        [ins3 outs3] (bindings-chain '[ [(inc ?c) ?d] [(str ?b "-" ?d) ?e] ]
                                     '#{?a ?b ?c}
                                     '[ [?a :attr ?c] [?x :label ?e] [?y :included true] ])]
    (is (= ins1 '[[(inc ?c) ?d] [(str ?b "-" ?d) ?e]]))
    (is (= outs1 '[[(dec ?c) ?f]]))
    (is (empty? ins2))
    (is (= outs2 '[[(inc ?c) ?d] [(str ?b "-" ?d) ?e] [(dec ?c) ?f]]))
    (is (= ins3 '[ [(inc ?c) ?d] [(str ?b "-" ?d) ?e] ]))
    (is (= outs3 '[]))))

(deftest test-first-group-evals
  (let [
        [in1 out1 ev1] (first-group nil '[ [?a :prop ?b] [?a :attr ?c]
                                           [?x :label ?e] [?y :included true] ]
                                        '[ [(inc ?c) ?d] [(str ?b "-" ?d) ?e] ])
        [in2 out2 ev2] (first-group nil '[ [?a :prop ?b] [?a :attr ?c]
                                           [?x :label ?e] [?y :included true] ]
                                        '[ [(dec ?c) ?f] [(inc ?c) ?d] [(str ?b "-" ?d) ?e] ])
        [in3 out3 ev3] (first-group nil '[ [?a :prop ?b] [?a :attr ?c]
                                           [?x :label ?e] [?y :included true] [?y :has ?f] ]
                                        '[ [(dec ?c) ?f] [(inc ?c) ?d] [(str ?b "-" ?d) ?e] ])
        ]
    (is (= in1 '[ [?a :prop ?b] [?a :attr ?c]
                  [(inc ?c) ?d] [(str ?b "-" ?d) ?e]
                  [?x :label ?e] ]))
    (is (= out1 '[ [?y :included true] ]))
    (is (= ev1 '[]))
    (is (= in2 '[ [?a :prop ?b] [?a :attr ?c]
                  [(inc ?c) ?d] [(str ?b "-" ?d) ?e]
                  [?x :label ?e] ]))
    (is (= out2 '[ [?y :included true] ]))
    (is (= ev2 '[ [(dec ?c) ?f] ]))
    (is (= in3 '[ [?a :prop ?b] [?a :attr ?c]
                  [(inc ?c) ?d] [(str ?b "-" ?d) ?e]
                  [?x :label ?e] [(dec ?c) ?f] [?y :has ?f] [?y :included true] ]))
    (is (= out3 '[]))
    (is (= ev3 '[]))))

(deftest test-query-path-evals
  (let [simple-p '[[?a :a :b] [?b :c :d]]
        simple-cm '{[?a :a :b] 1, [?b :c :d] 1}
        [g] (first-group nil simple-p '[[(identity ?a) ?c]])
        p (min-join-path nil simple-cm simple-p '[[(identity ?a) ?c]])

        simple-p2 '[[?a :a :b] [?b :c :d]]
        simple-cm2 '{[?a :a :b] 1, [?b :c :d] 1}
        [g2] (first-group nil simple-p2 '[[(identity ?a) ?b]])
        p2 (min-join-path nil simple-cm2 simple-p2 '[[(identity ?a) ?b]])

        simple-p3 '[[?a :a :b] [?b :c :d]]
        simple-cm3 '{[?a :a :b] 1, [?b :c :d] 1}
        [g3] (first-group nil simple-p3 '[[(identity ?b) ?a]])
        p3 (min-join-path nil simple-cm3 simple-p3 '[[(identity ?b) ?a]])

        simple-p4 '[[?a :a :b] [?b :c :d] [?c :e ?b] [?a :c :d]]
        simple-cm4 '{[?a :a :b] 1, [?b :c :d] 2, [?c :e ?b] 1, [?a :c :d] 1}
        [g4] (first-group nil simple-p4 '[[(inc ?b) ?z]])
        p4 (min-join-path nil simple-cm4 simple-p4 '[[(inc ?b) ?z]])

        simple-p5 '[[?a :a :b] [?b :c :d] [?c :e ?b] [?a :c :d]]
        simple-cm5 '{[?a :a :b] 4, [?b :c :d] 5, [?c :e ?b] 1, [?a :c :d] 3}
        [g5] (first-group nil simple-p5 '[[(inc ?b) ?z]])
        p5 (min-join-path nil simple-cm5 simple-p5 '[[(inc ?b) ?z]])

        simple-p6 '[[?a :a :b] [?b :c :d] [?c :e ?b] [?a :c :d]]
        simple-cm6 '{[?a :a :b] 4, [?b :c :d] 5, [?c :e ?b] 1, [?a :c :d] 3}
        [g6] (first-group nil simple-p6 '[[(inc ?a) ?b]])
        p6 (min-join-path nil simple-cm6 simple-p6 '[[(inc ?a) ?b]])

        simple-p7 '[[?a :a :b] [?b :c :d] [?c :e ?b] [?a :c :d]]
        simple-cm7 '{[?a :a :b] 4, [?b :c :d] 5, [?c :e ?b] 1, [?a :c :d] 3}
        [g7] (first-group nil simple-p7 '[[(inc ?b) ?a]])
        p7 (min-join-path nil simple-cm7 simple-p7 '[[(inc ?b) ?a]])

        simple-p8 '[[?a :a :b] [?b :c :d] [?c :e ?b] [?a :c :d]]
        simple-cm8 '{[?a :a :b] 4, [?b :c :d] 2, [?c :e ?b] 1, [?a :c :d] 3}
        [g8] (first-group nil simple-p8 '[[(inc ?b) ?a]])
        p8 (min-join-path nil simple-cm8 simple-p8 '[[(inc ?b) ?a]])

        simple-p9 '[[?a :a :b] [?b :c :d] [?c :e ?b] [?a :c :d]]
        simple-cm9 '{[?a :a :b] 3, [?b :c :d] 5, [?c :e ?b] 4, [?a :c :d] 2}
        [g9] (first-group nil simple-p9 '[[(inc ?a) ?b]])
        p9 (min-join-path nil simple-cm9 simple-p9 '[[(inc ?a) ?b]])]

    (is (= '[[?a :a :b]] g))
    (is (= '[[?a :a :b] [?b :c :d] [(identity ?a) ?c]] p))

    (is (= '[[?a :a :b] [(identity ?a) ?b] [?b :c :d]] g2))
    (is (= '[[?a :a :b] [(identity ?a) ?b] [?b :c :d]] p2))

    (is (= '[[?b :c :d] [(identity ?b) ?a] [?a :a :b]] g3))
    (is (= '[[?b :c :d] [(identity ?b) ?a] [?a :a :b]] p3))

    (is (= '[[?a :a :b] [?a :c :d]] g4))
    (is (= '[[?a :a :b] [?a :c :d] [?c :e ?b] [?b :c :d] [(inc ?b) ?z]] p4))

    (is (= '[[?a :a :b] [?a :c :d]] g5))
    (is (= '[[?a :c :d] [?a :a :b] [?c :e ?b] [?b :c :d] [(inc ?b) ?z]] p5))

    (is (= '[[?a :a :b] [?a :c :d] [(inc ?a) ?b] [?b :c :d] [?c :e ?b]] g6))
    (is (= '[[?a :c :d] [(inc ?a) ?b] [?c :e ?b] [?a :a :b] [?b :c :d]] p6))

    (is (= '[[?b :c :d] [?c :e ?b] [(inc ?b) ?a] [?a :a :b] [?a :c :d]] g7))
    (is (= '[[?c :e ?b] [(inc ?b) ?a] [?a :c :d] [?a :a :b] [?b :c :d]] p7))

    (is (= '[[?b :c :d] [?c :e ?b] [(inc ?b) ?a] [?a :a :b] [?a :c :d]] g8))
    (is (= '[[?c :e ?b] [?b :c :d] [(inc ?b) ?a] [?a :c :d] [?a :a :b]] p8))

    (is (= '[[?a :a :b] [?a :c :d] [(inc ?a) ?b] [?b :c :d] [?c :e ?b]] g9))
    (is (= '[[?a :c :d] [?a :a :b] [(inc ?a) ?b] [?c :e ?b] [?b :c :d]] p9))))

(defrecord TestGraph [m]
  Graph
  (count-triple [g s p o]
    (get m [s p o])))

(deftest test-query-path-negations
  (let [test-patterns '[[?a :a :b] (not [?a :c :d]) [(identity ?a) ?c]]
        test-graph (->TestGraph '{[?a :a :b] 1, [?a :c :d] 1})
        p (plan-path test-graph test-patterns {})]
    (is (= '[[?a :a :b] (not [?a :c :d]) [(identity ?a) ?c]] p)))

  (let [test-patterns '[[?a :a :b] [?b :c :d] (not [?a :e :f]) [(identity ?a) ?b]]
        test-graph (->TestGraph '{[?a :a :b] 1, [?b :c :d] 1, [?a :e :f] 1})
        p2 (plan-path test-graph test-patterns {})]
    (is (= '[[?a :a :b] (not [?a :e :f]) [(identity ?a) ?b] [?b :c :d]] p2)))

  (let [test-patterns '[[?a :a :b] [?b :c :d] (not [?b :e :f]) [(identity ?a) ?b]]
        test-graph (->TestGraph '{[?a :a :b] 1, [?b :c :d] 1, [?b :e :f] 1})
        p3 (plan-path test-graph test-patterns {})]
    (is (= '[[?a :a :b] [(identity ?a) ?b] (not [?b :e :f]) [?b :c :d]] p3)))

  (let [test-patterns '[[?a :a :b] [?b :c :d] [?c :e ?b] [?a :c :d] (not [?b :s :o]) [(inc ?a) ?b]]
        test-graph (->TestGraph '{[?a :a :b] 4, [?b :c :d] 5, [?c :e ?b] 1, [?a :c :d] 3, [?b :s :o] 1})
        p6 (plan-path test-graph test-patterns {})]
    (is (= '[[?a :c :d] [(inc ?a) ?b] (not [?b :s :o]) [?c :e ?b] [?a :a :b] [?b :c :d]] p6)))

  (let [test-patterns '[[?a :a :b] [?b :c :d] [?c :e ?b] [?a :c :d] (not [?c :s :o]) [(inc ?a) ?b]]
        test-graph (->TestGraph '{[?a :a :b] 4, [?b :c :d] 5, [?c :e ?b] 1, [?a :c :d] 3, [?c :s :o] 1})
        p6 (plan-path test-graph test-patterns {})]
    (is (= '[[?a :c :d] [(inc ?a) ?b] [?c :e ?b] (not [?c :s :o]) [?a :a :b] [?b :c :d]] p6)))

  (let [test-patterns '[[?a :a :b] [?b :c :d] [?c :e ?b] [?a :c :d] (not [?c :s ?z] [?z :x :y]) [(inc ?a) ?b]]
        test-graph (->TestGraph '{[?a :a :b] 4, [?b :c :d] 5, [?c :e ?b] 1, [?a :c :d] 2, [?c :s ?z] 1, [?z :x :y] 4})
        p8 (plan-path test-graph test-patterns {})]
    (is (= '[[?a :c :d] [(inc ?a) ?b] [?c :e ?b] (not [?c :s ?z] [?z :x :y]) [?a :a :b] [?b :c :d]] p8)))
  
  (let [test-patterns '[[?a :a :b] [?b :c :d] [?c :e ?b] [?a :c :d] (not [?c :s ?z] [?z :x :y]) [(inc ?a) ?b]]
        test-graph (->TestGraph '{[?a :a :b] 4, [?b :c :d] 5, [?c :e ?b] 1, [?a :c :d] 2, [?c :s ?z] 4, [?z :x :y] 1})
        p9 (plan-path test-graph test-patterns {})]
    (is (= '[[?a :c :d] [(inc ?a) ?b] [?c :e ?b] (not [?c :s ?z] [?z :x :y]) [?a :a :b] [?b :c :d]] p9)))

  (let [test-patterns '[[?a :a :b] [?b :c :d] [?c :e ?b] [?a :c :d]
                        (not [?z :x :y] [?e :s ?z])
                        [(identity ?c) ?e] [(inc ?a) ?b]]
        test-graph (->TestGraph '{[?a :a :b] 4, [?b :c :d] 5, [?c :e ?b] 1, [?a :c :d] 2, [?e :s ?z] 4, [?z :x :y] 1})
        p10 (plan-path test-graph test-patterns {})]
    (is (= '[[?a :c :d] [(inc ?a) ?b] [?c :e ?b] [?a :a :b] [?b :c :d] [(identity ?c) ?e] (not [?e :s ?z] [?z :x :y])] p10))))

#?(:cljs (run-tests))
