(ns asami.test-memory
  #?(:clj  (:require [naga.store :refer [assert-data resolve-pattern count-pattern query start-tx commit-tx deltas]]
                     [asami.core :refer [empty-store first-group min-join-path merge-filters]]
                     [clojure.test :as t :refer [testing is run-tests]]
                     [asami.util :as u]
                     [schema.test :as st :refer [deftest]])
     :cljs (:require [naga.store :refer [assert-data resolve-pattern count-pattern query start-tx commit-tx deltas]]
                     [asami.core :refer [empty-store first-group min-join-path merge-filters]]
                     [asami.util :as u]
                     [clojure.test :as t :refer-macros [testing is run-tests]]
                     [schema.test :as st :refer-macros [deftest]])))

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

(defn unordered-resolve
  [g pattern]
  (into #{} (resolve-pattern g pattern)))

(deftest test-load
  (let [s (assert-data empty-store data)
        r1 (unordered-resolve s '[:a ?a ?b])
        r2 (unordered-resolve s '[?a :p2 ?b])
        r3 (unordered-resolve s '[:a :p1 ?a])
        r4 (unordered-resolve s '[?a :p2 :z])
        r5 (unordered-resolve s '[:a ?a :x])
        r6 (unordered-resolve s '[:a :p4 ?a])
        r7 (unordered-resolve s '[:c :p4 :t])
        s' (assert-data s [[:d :p2 :z] [:a :p4 :y]])
        r8 (unordered-resolve s' '[:a ?a ?b])
        r9 (unordered-resolve s' '[?a :p2 ?b])]
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
    (is (empty? r6))
    (is (= #{[]} r7))
    (is (= #{[:p1 :x]
             [:p1 :y]
             [:p2 :z]
             [:p3 :x]
             [:p4 :y]} r8))
    (is (= #{[:a :z]
             [:b :x]
             [:d :z]} r9))))

(deftest test-count
  (let [s (assert-data empty-store data)
        r1 (count-pattern s '[:a ?a ?b])
        r2 (count-pattern s '[?a :p2 ?b])
        r3 (count-pattern s '[:a :p1 ?a])
        r4 (count-pattern s '[?a :p2 :z])
        r5 (count-pattern s '[:a ?a :x])
        r6 (count-pattern s '[:a :p4 ?a])
        r7 (count-pattern s '[:c :p4 :t])
        s' (assert-data s [[:d :p2 :z] [:a :p4 :y]])
        r8 (count-pattern s' '[:a ?a ?b])
        r9 (count-pattern s' '[?a :p2 ?b])]
    (is (= 4 r1))
    (is (= 2 r2))
    (is (= 2 r3))
    (is (= 1 r4))
    (is (= 2 r5))
    (is (zero? r6))
    (is (= 1 r7))
    (is (= 5 r8))
    (is (= 3 r9))))

(def id-data
  [[:mem/node-1 :p1 :x]
   [:mem/node-1 :p1 :y]
   [:mem/node-1 :p2 :z]
   [:mem/node-1 :p3 :x]
   [:mem/node-2 :p1 :x]
   [:mem/node-2 :p2 :x]
   [:mem/node-2 :p3 :z]
   [:mem/node-3 :p4 :t]])


(def data-entities (map (fn [x] [x :naga/entity true]) [:mem/node-1 :mem/node-2 :mem/node-3]))

(deftest test-tx
  (let [s1 (assert-data empty-store (concat id-data data-entities))
        s2 (start-tx s1)
        s3 (assert-data s2 [[:d :p2 :z] [:mem/node-3 :p4 :z] [:mem/node-1 :p4 :y]])
        s4 (commit-tx s3)
        r1 (unordered-resolve s4 '[:mem/node-1 ?a ?b])
        r2 (unordered-resolve s4 '[?a :p2 ?b])
        r3 (set (deltas s4))]
    (is (= #{[:p1 :x]
             [:p1 :y]
             [:p2 :z]
             [:p3 :x]
             [:p4 :y]
             [:naga/entity true]} r1))
    (is (= #{[:mem/node-1 :z]
             [:mem/node-2 :x]
             [:d :z]} r2))
    (is (= #{:mem/node-1 :mem/node-3} r3))))

(def jdata
  [[:a :p1 :x]
   [:a :p1 :y]
   [:a :p2 :z]
   [:a :p3 :x]
   [:b :p1 :x]
   [:b :p2 :x]
   [:b :p3 :z]
   [:c :p4 :t]
   [:x :q1 :l]
   [:x :q2 :m]
   [:y :q1 :l]
   [:y :q3 :n]])

(defn unordered-query
  [s op pattern]
  (into #{} (query s op pattern)))

(deftest test-join
  (let [s (assert-data empty-store jdata)
        r1 (unordered-query s '[?a ?b ?d] '[[:a ?a  ?b] [?b ?c  ?d]])
        r2 (unordered-query s '[?a ?b ?d] '[[?a ?b  :x] [:a ?b  ?d]])
        r3 (unordered-query s '[?x ?y]    '[[:a :p1 ?x] [:y :q1 ?y]])
        r4 (unordered-query s '[?x]       '[[:a :p1 ?x] [:y :q1 :l]])
        r5 (unordered-query s '[?x ?y]    '[[:a :p1 ?x] [:y ?y  ?z]])]
    (is (= #{[:p1 :x :l]
             [:p1 :x :m]
             [:p1 :y :l]
             [:p1 :y :n]
             [:p3 :x :l]
             [:p3 :x :m]} r1))
    (is (= #{[:a :p1 :x]
             [:a :p1 :y]       
             [:a :p3 :x]
             [:b :p1 :x]
             [:b :p1 :y]
             [:b :p2 :z]} r2))
    (is (= #{[:x :l]
             [:y :l]} r3))
    (is (= #{[:x]
             [:y]} r4))
    (is (= #{[:x :q1]
             [:x :q3]
             [:y :q1]
             [:y :q3]} r5))))


(def j2data
  [[:a :p1 :b]
   [:a :p1 :b]
   [:a :p2 :z]
   [:a :p3 :x]
   [:b :px :c]
   [:b :px :d]
   [:b :py :c]
   [:c :pa :t]
   [:c :pb :u]
   [:d :pz :l]
   [:x :q2 :m]
   [:y :q1 :l]
   [:y :q3 :n]])

(deftest test-multi-join
  (let [s (assert-data empty-store j2data)
        r1 (unordered-query s '[?p ?v] '[[:a ?a ?b] [?b :px ?d] [?d ?p ?v]])]
    (is (= #{[:pa :t]
             [:pb :u]
             [:pz :l]} r1))))

(deftest test-query-path
  (let [simple-p '[[?a :a :b] [?b :c :d]]
        simple-cm '{[?a :a :b] 1, [?b :c :d] 1}
        [g] (first-group simple-p)
        p (min-join-path simple-p simple-cm)
        simple-p2 '[[?a :a :b] [?b :c :d] [?c :e ?b] [?a :c :d]]
        simple-cm2 '{[?a :a :b] 1, [?b :c :d] 2, [?c :e ?b] 1, [?a :c :d] 1}
        [g2] (first-group simple-p2)
        p2 (min-join-path simple-p2 simple-cm2)
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
        [group] (first-group patterns)
        path (min-join-path patterns count-map)]

    (is (= '[[?a :a :b]] g))
    (is (= '[[?a :a :b] [?b :c :d]] p))

    (is (= '[[?a :a :b] [?a :c :d]] g2))
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

(def j3data
  [[:a :p1 :b]
   [:a :p1 :b]
   [:a :p2 :z]
   [:a :p3 :x]
   [:a :p3 :z]
   [:b :px :c]
   [:b :px :d]
   [:b :py :c]
   [:c :pa :t]
   [:c :pb :u]
   [:d :pz :l]
   [:x :q2 :m]
   [:y :q1 :l]
   [:y :q3 :n]
   [:y :q1 :b]])

(deftest test-merge-filters
  (is (= '[[:a ?a ?b] (= ?b :z)] (merge-filters '[[:a ?a ?b]] '[(= ?b :z)])))
  (is (= '[[:x ?c ?a] [:a ?a ?b] (= ?b :z)] (merge-filters '[[:x ?c ?a] [:a ?a ?b]] '[(= ?b :z)])))
  (is (= '[[:x ?c ?a] (= ?a :z) [:a ?a ?b]] (merge-filters '[[:x ?c ?a] [:a ?a ?b]] '[(= ?a :z)]))))

#?(:clj
   (deftest test-eval
     (is (= 5 (u/c-eval 5)))
     (is ((u/c-eval '(fn [[?a ?b]] (= ?b :z))) [0 :z]))))

#?(:clj
   (deftest test-filter
     []
     (let [s (assert-data empty-store j3data)
           r1 (unordered-query s '[?a] '[[:a ?a ?b] (= ?b :z)])
           r2 (unordered-query s '[?a ?b] '[[:a ?a ?b] [?b :px ?d] (= ?d :c)])
           r3 (unordered-query s '[?d] '[[:a ?a ?b] [?b :px ?d] (not= ?d :c)])
           r4 (unordered-query s '[?z] '[[?z :p1 ?x] [:y :q1 ?y] (= ?x ?y)])]
       (is (= #{[:p2] [:p3]} r1))
       (is (= #{[:p1 :b]} r2))
       (is (= #{[:d]} r3))
       (is (= #{[:a]} r4)))))

#?(:cljs (run-tests))

