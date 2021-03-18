(ns asami.durable.transitive-test
  "Tests internals of the query portion of the memory storage"
  (:require
            [asami.graph :refer [graph-transact resolve-triple]]
            [asami.durable.graph-test :refer [make-graph remove-group]]
            [asami.durable.test-utils :as util :include-macros true]
            #?(:clj  [schema.core :as s]
               :cljs [schema.core :as s :include-macros true])
            #?(:clj  [clojure.test :refer [is use-fixtures testing]]
               :cljs [clojure.test :refer-macros [is run-tests use-fixtures testing]])
            #?(:clj  [schema.test :as st :refer [deftest]]
               :cljs [schema.test :as st :refer-macros [deftest]]))
  #?(:clj (:import [clojure.lang ExceptionInfo])))

(use-fixtures :once st/validate-schemas)

(def default-tx 1)

(defn init-graph
  [nm data]
  (let [g (make-graph nm)]
    (graph-transact g default-tx data nil)))

(defn unordered-resolve
  [g [s p o]]
  (into #{} (resolve-triple g s p o)))

(defn t [x] (with-meta x {:trans :plus}))

(deftest test-simple
  (util/with-cleanup [g (init-graph "transitive-simple" [[:a :p1 :b] [:b :p1 :c]])]
    (let [r1 (unordered-resolve g '[:a :p1+ ?x])
          r1' (unordered-resolve g '[:a :p1* ?x])
          r2 (unordered-resolve g '[?x :p1+ :c])
          r2' (unordered-resolve g '[?x :p1* :c])
          r3 (unordered-resolve g '[:a ?p+ ?x])
          r3' (unordered-resolve g '[:a ?p* ?x])
          r4 (unordered-resolve g '[?x ?p+ :c])
          r4' (unordered-resolve g '[?x ?p* :c])
          r5 (unordered-resolve g [:a (t '?p) '?x])  ;; duplicate of above, with alternate syntax
          r6 (unordered-resolve g ['?x (t '?p) :c])]
      (is (= #{[:b] [:c]} r1))
      (is (= #{[:a] [:b] [:c]} r1'))
      (is (= #{[:b] [:a]} r2))
      (is (= #{[:c] [:b] [:a]} r2'))
      (is (= #{[:p1 :b] [:p1 :c]} r3))
      (is (= #{[:p1 :a] [:p1 :b] [:p1 :c]} r3'))
      (is (= #{[:b :p1] [:a :p1]} r4))
      (is (= #{[:c :p1] [:b :p1] [:a :p1]} r4'))
      (is (= #{[:p1 :b] [:p1 :c]} r5))
      (is (= #{[:b :p1] [:a :p1]} r6))))
  (remove-group "transitive-simple"))

(deftest test-simple-path
  (util/with-cleanup [g (init-graph "transitive-simple-path" [[:a :p1 :b] [:b :p1 :c]])]
    (let [r1 (resolve-triple g :a '?p* :c)]
      (is (= [[[:p1 :p1]]] r1))))
  (remove-group "transitive-simple-path"))

(def simple-branch-data
  [[:a :p1 :b]
   [:b :p1 :c]
   [:b :p1 :d]
   [:c :p1 :e]
   [:d :p1 :f]])

(deftest test-branch
  (util/with-cleanup [g (init-graph "transitive-branch" simple-branch-data)]
    (let [r1 (unordered-resolve g '[:a :p1+ ?x])
          r1' (unordered-resolve g '[:a :p1* ?x])
          r2 (unordered-resolve g '[?x :p1+ :e])
          r2' (unordered-resolve g '[?x :p1* :e])
          r3 (unordered-resolve g '[:a ?p+ ?x])
          r3' (unordered-resolve g '[:a ?p* ?x])
          r4 (unordered-resolve g '[?x ?p+ :e])
          r4' (unordered-resolve g '[?x ?p* :e])
          r5 (unordered-resolve g '[:b :p1+ ?x])
          r5' (unordered-resolve g '[:b :p1* ?x])
          r6 (unordered-resolve g '[?x :p1+ :c])
          r6' (unordered-resolve g '[?x :p1* :c])
          r7 (unordered-resolve g '[:b ?p+ ?x])
          r7' (unordered-resolve g '[:b ?p* ?x])
          r8 (unordered-resolve g '[?x ?p+ :c])
          r8' (unordered-resolve g '[?x ?p* :c])]
      (is (= #{[:b] [:c] [:d] [:e] [:f]} r1))
      (is (= #{[:a] [:b] [:c] [:d] [:e] [:f]} r1'))
      (is (= #{[:c] [:b] [:a]} r2))
      (is (= #{[:e] [:c] [:b] [:a]} r2'))
      (is (= #{[:p1 :b] [:p1 :c] [:p1 :d] [:p1 :e] [:p1 :f]} r3))
      (is (= #{[:p1 :a] [:p1 :b] [:p1 :c] [:p1 :d] [:p1 :e] [:p1 :f]} r3'))
      (is (= #{[:c :p1] [:b :p1] [:a :p1]} r4))
      (is (= #{[:e :p1] [:c :p1] [:b :p1] [:a :p1]} r4'))
      (is (= #{[:c] [:d] [:e] [:f]} r5))
      (is (= #{[:b] [:c] [:d] [:e] [:f]} r5'))
      (is (= #{[:b] [:a]} r6))
      (is (= #{[:c] [:b] [:a]} r6'))
      (is (= #{[:p1 :c] [:p1 :d] [:p1 :e] [:p1 :f]} r7))
      (is (= #{[:p1 :b] [:p1 :c] [:p1 :d] [:p1 :e] [:p1 :f]} r7'))
      (is (= #{[:b :p1] [:a :p1]} r8))
      (is (= #{[:c :p1] [:b :p1] [:a :p1]} r8'))))
  (remove-group "transitive-branch"))

(deftest branch-path
  (util/with-cleanup [g (init-graph "transitive-branch-path" simple-branch-data)]
    (let [r1 (resolve-triple g :a '?p* :c)
          r2 (resolve-triple g :a '?p* :e)]
      (is (= [[[:p1 :p1]]] r1))
      (is (= [[[:p1 :p1 :p1]]] r2))))
  (remove-group "transitive-branch-path"))

(def dbl-branch-data
  [[:a :p1 :b]
   [:b :p1 :c]
   [:b :p2 :c]
   [:b :p1 :d]
   [:c :p2 :e]
   [:d :p2 :f]
   [:g :p2 :h]])

(deftest test-dbl-branch
  (util/with-cleanup [g (init-graph "transitive-dbl-branch" dbl-branch-data)]
    (let [r1 (unordered-resolve g '[:a :p1+ ?x])
          r1' (unordered-resolve g '[:a :p1* ?x])
          r2 (unordered-resolve g '[?x :p2+ :e])
          r2' (unordered-resolve g '[?x :p2* :e])
          r3 (unordered-resolve g '[:a ?p+ ?x])
          r3' (unordered-resolve g '[:a ?p* ?x])
          r4 (unordered-resolve g '[?x ?p+ :e])
          r4' (unordered-resolve g '[?x ?p* :e])
          r5 (unordered-resolve g '[:b :p1+ ?x])
          r5' (unordered-resolve g '[:b :p1* ?x])
          r6 (unordered-resolve g '[?x :p1+ :c])
          r6' (unordered-resolve g '[?x :p1* :c])
          r7 (unordered-resolve g '[:b ?p+ ?x])
          r7' (unordered-resolve g '[:b ?p* ?x])
          r8 (unordered-resolve g '[?x ?p+ :c])
          r8' (unordered-resolve g '[?x ?p* :c])]
      (is (= #{[:b] [:c] [:d]} r1))
      (is (= #{[:a] [:b] [:c] [:d]} r1'))
      (is (= #{[:c] [:b]} r2))
      (is (= #{[:e] [:c] [:b]} r2'))
      (is (= #{[:p1 :b] [:p1 :c] [:p1 :d] [:p1 :e] [:p1 :f]} r3))
      (is (= #{[:p1 :a] [:p1 :b] [:p1 :c] [:p1 :d] [:p1 :e] [:p1 :f]} r3'))
      (is (= #{[:c :p2] [:b :p2] [:a :p2]} r4))
      (is (= #{[:e :p2] [:c :p2] [:b :p2] [:a :p2]} r4'))
      (is (= #{[:c] [:d]} r5))
      (is (= #{[:b] [:c] [:d]} r5'))
      (is (= #{[:b] [:a]} r6))
      (is (= #{[:c] [:b] [:a]} r6'))
      (is (= #{[:p1 :c] [:p2 :c] [:p1 :d] [:p1 :e] [:p2 :e] [:p1 :f]} r7))
      (is (= #{[:p1 :b] [:p2 :b] [:p1 :c] [:p2 :c] [:p1 :d] [:p1 :e] [:p2 :e] [:p1 :f]} r7'))
      (is (= #{[:b :p1] [:b :p2] [:a :p1] [:a :p2]} r8))
      (is (= #{[:c :p1] [:c :p2] [:b :p1] [:b :p2] [:a :p1] [:a :p2]} r8'))))
  (remove-group "transitive-dbl-branch"))

(deftest dbl-branch-path
  (util/with-cleanup [g (init-graph "transitive-dbl-branch-path" dbl-branch-data)]
    (let [r1 (resolve-triple g :a '?p* :c)
          r2 (resolve-triple g :a '?p* :f)
          r3 (resolve-triple g :a '?p* :h)]
      (is (= [[[:p1 :p1]]] r1))
      (is (= [[[:p1 :p1 :p2]]] r2))
      (is (= [] r3))))
  (remove-group "transitive-dbl-branch-path"))

(def simple-loop-data
  [[:a :p1 :b]
   [:b :p1 :c]
   [:c :p1 :a]])

(deftest data-loop
  (util/with-cleanup [g (init-graph "transitive-loop" simple-loop-data)]
    (let [r1 (unordered-resolve g '[:a :p1+ ?x])
          r1' (unordered-resolve g '[:a :p1* ?x])
          r2 (unordered-resolve g '[?x :p1+ :a])
          r2' (unordered-resolve g '[?x :p1* :a])
          r3 (unordered-resolve g '[:a ?p+ ?x])
          r3' (unordered-resolve g '[:a ?p* ?x])
          r4 (unordered-resolve g '[?x ?p+ :a])
          r4' (unordered-resolve g '[?x ?p* :a])]
      (is (= #{[:b] [:c] [:a]} r1))
      (is (= #{[:b] [:c] [:a]} r1'))
      (is (= #{[:c] [:b] [:a]} r2))
      (is (= #{[:c] [:b] [:a]} r2'))
      (is (= #{[:p1 :b] [:p1 :c] [:p1 :a]} r3))
      (is (= #{[:p1 :b] [:p1 :c] [:p1 :a]} r3'))
      (is (= #{[:c :p1] [:b :p1] [:a :p1]} r4))
      (is (= #{[:c :p1] [:b :p1] [:a :p1]} r4'))))
  (remove-group "transitive-loop"))

(deftest test-loop-path
  (util/with-cleanup [g (init-graph "transitive-loop-path" simple-loop-data)]
    (let [r1 (resolve-triple g :a '?p* :c)
          r2 (resolve-triple g :a '?p* :a)
          r3 (resolve-triple g :a '?p+ :a)]
      (is (= [[[:p1 :p1]]] r1))
      (is (= [[[]]] r2))
      (is (= [[[:p1 :p1 :p1]]] r3))))
  (remove-group "transitive-loop-path"))

(def dbl-branch-loop-data
  [[:a :p1 :b]
   [:b :p1 :c]
   [:b :p2 :c]
   [:b :p1 :d]
   [:d :p1 :a]
   [:c :p2 :e]
   [:e :p2 :b]
   [:d :p2 :f]
   [:g :p2 :h]])

(deftest test-dbl-branch-loop
  (util/with-cleanup [g (init-graph "transitive-dbl-branch-loop" dbl-branch-loop-data)]
    (let [r1 (unordered-resolve g '[:a :p1+ ?x])
          r1' (unordered-resolve g '[:a :p1* ?x])
          r2 (unordered-resolve g '[?x :p2+ :e])
          r2' (unordered-resolve g '[?x :p2* :e])
          r3 (unordered-resolve g '[:a ?p+ ?x])
          r3' (unordered-resolve g '[:a ?p* ?x])
          r4 (unordered-resolve g '[?x ?p+ :e])
          r4' (unordered-resolve g '[?x ?p* :e])
          r5 (unordered-resolve g '[:b :p1+ ?x])
          r5' (unordered-resolve g '[:b :p1* ?x])
          r6 (unordered-resolve g '[?x :p1+ :c])
          r6' (unordered-resolve g '[?x :p1* :c])
          r7 (unordered-resolve g '[:b ?p+ ?x])
          r7' (unordered-resolve g '[:b ?p* ?x])
          r8 (unordered-resolve g '[?x ?p+ :c])
          r8' (unordered-resolve g '[?x ?p* :c])]
      (is (= #{[:b] [:c] [:d] [:a]} r1))
      (is (= #{[:b] [:c] [:d] [:a]} r1'))
      (is (= #{[:c] [:b] [:e]} r2))
      (is (= #{[:c] [:b] [:e]} r2'))
      (is (= #{[:p1 :b] [:p1 :c] [:p1 :d] [:p1 :a] [:p1 :e] [:p1 :f]} r3))
      (is (= #{[:p1 :b] [:p1 :c] [:p1 :d] [:p1 :a] [:p1 :e] [:p1 :f]} r3'))
      (is (= #{[:c :p2] [:b :p2] [:a :p2] [:e :p2] [:d :p2]} r4))
      (is (= #{[:c :p2] [:b :p2] [:a :p2] [:e :p2] [:d :p2]} r4'))
      (is (= #{[:c] [:d] [:a] [:b]} r5))
      (is (= #{[:c] [:d] [:a] [:b]} r5'))
      (is (= #{[:b] [:a] [:d]} r6))
      (is (= #{[:c] [:b] [:a] [:d]} r6'))
      (is (= #{[:p1 :c] [:p2 :c] [:p1 :d] [:p2 :d] [:p1 :e] [:p2 :e]
               [:p1 :b] [:p2 :b] [:p1 :f] [:p2 :f] [:p1 :a] [:p2 :a]} r7))
      (is (= #{[:p1 :c] [:p2 :c] [:p1 :d] [:p2 :d] [:p1 :e] [:p2 :e]
               [:p1 :b] [:p2 :b] [:p1 :f] [:p2 :f] [:p1 :a] [:p2 :a]} r7'))
      (is (= #{[:b :p1] [:b :p2] [:a :p1] [:a :p2] [:d :p1] [:d :p2]
               [:c :p1] [:c :p2] [:e :p1] [:e :p2]} r8))
      (is (= #{[:b :p1] [:b :p2] [:a :p1] [:a :p2] [:d :p1] [:d :p2]
               [:c :p1] [:c :p2] [:e :p1] [:e :p2]} r8'))))
  (remove-group "transitive-dbl-branch-loop"))

(deftest test-loop-variants
  (util/with-cleanup [g1 (init-graph "loop-variants1" (remove #(= '[:b :p1 :c] %) dbl-branch-loop-data))]
    (let [r1 (unordered-resolve g1 '[:b ?p* ?x])]
      (is (= #{[:p1 :c] [:p2 :c] [:p1 :d] [:p2 :d] [:p1 :e] [:p2 :e]
               [:p1 :b] [:p2 :b] [:p1 :f] [:p2 :f] [:p1 :a] [:p2 :a]} r1))))
  (remove-group "loop-variants1")
  (util/with-cleanup [g2 (init-graph "loop-variants2" (remove #(= '[:d :p1 :a] %) dbl-branch-loop-data))]
    (let [r2 (unordered-resolve g2 '[:b ?p* ?x])]
      (is (= #{[:p1 :c] [:p2 :c] [:p1 :d] [:p2 :d] [:p1 :e] [:p2 :e]
               [:p1 :b] [:p2 :b] [:p1 :f] [:p2 :f]} r2))))
  (remove-group "loop-variants2")
  (util/with-cleanup [g3 (init-graph "loop-variants3" (remove #(= '[:b :p2 :c] %) dbl-branch-loop-data))]
    (let [r3 (unordered-resolve g3 '[:b ?p* ?x])]
      (is (= #{[:p1 :c] [:p1 :d] [:p1 :e]
               [:p1 :b] [:p1 :f] [:p1 :a]} r3))))
  (remove-group "loop-variants3"))

(deftest test-dbl-branch-loop-path
  (util/with-cleanup [g (init-graph "transitive-dbl-branch-loop-path" dbl-branch-loop-data)]
    (let [r1 (resolve-triple g :a '?p* :c)
          r2 (resolve-triple g :a '?p* :f)
          r3 (resolve-triple g :a '?p* :h)]
      (is (= [[[:p1 :p1]]] r1))
      (is (= [[[:p1 :p1 :p2]]] r2))
      (is (= [] r3))))
  (remove-group "transitive-dbl-branch-loop-path"))
