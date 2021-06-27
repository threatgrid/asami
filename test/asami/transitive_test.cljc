(ns asami.transitive-test
  "Tests internals of the query portion of the memory storage"
  (:require
            [clojure.set :as set]
            [asami.graph :refer [Graph graph-add resolve-pattern]]
            [asami.index :refer [empty-graph]]
            [asami.multi-graph :refer [empty-multi-graph]]
            [asami.common-index :refer [step-by-predicate]]
            #?(:clj  [schema.core :as s]
               :cljs [schema.core :as s :include-macros true])
            #?(:clj  [clojure.test :refer [is use-fixtures testing]]
               :cljs [clojure.test :refer-macros [is run-tests use-fixtures testing]])
            #?(:clj  [schema.test :as st :refer [deftest]]
               :cljs [schema.test :as st :refer-macros [deftest]]))
  #?(:clj (:import [clojure.lang ExceptionInfo])))

(use-fixtures :once st/validate-schemas)

(def default-tx 1)

(defn assert-data [g d]
  (reduce (fn [g [s p o]] (graph-add g s p o default-tx)) g d))

(defn unordered-resolve
  [g pattern]
  (into #{} (resolve-pattern g pattern)))

(defn t [x] (with-meta x {:trans :plus}))

(defn simple [g]
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

(deftest test-simple
  (let [g (assert-data empty-graph [[:a :p1 :b] [:b :p1 :c]])
        gm (assert-data empty-multi-graph [[:a :p1 :b] [:b :p1 :c]])]
    (simple g)
    (simple gm)))

(defn simple-path [g]
  (let [r1 (resolve-pattern g '[:a ?p* :c])]
    (is (= [[[:p1 :p1]]] r1))))

(deftest test-simple-path
  (let [g (assert-data empty-graph [[:a :p1 :b] [:b :p1 :c]])
        gm (assert-data empty-multi-graph [[:a :p1 :b] [:b :p1 :c]])]
    (simple-path g)
    (simple-path gm)))

(def simple-branch-data
  [[:a :p1 :b]
   [:b :p1 :c]
   [:b :p1 :d]
   [:c :p1 :e]
   [:d :p1 :f]])

(defn branch [g]
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

(deftest test-branch
  (let [g (assert-data empty-graph simple-branch-data)
        gm (assert-data empty-multi-graph simple-branch-data)]
    (branch g)
    (branch gm)))

(defn branch-path [g]
  (let [r1 (resolve-pattern g '[:a ?p* :c])
        r2 (resolve-pattern g '[:a ?p* :e])]
    (is (= [[[:p1 :p1]]] r1))
    (is (= [[[:p1 :p1 :p1]]] r2))))

(deftest test-branch-path
  (let [g (assert-data empty-graph simple-branch-data)
        gm (assert-data empty-multi-graph simple-branch-data)]
    (branch-path g)
    (branch-path gm)))

(def dbl-branch-data
  [[:a :p1 :b]
   [:b :p1 :c]
   [:b :p2 :c]
   [:b :p1 :d]
   [:c :p2 :e]
   [:d :p2 :f]
   [:g :p2 :h]])

(defn dbl-branch [g]
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

(deftest test-dbl-branch
  (let [g (assert-data empty-graph dbl-branch-data)
        gm (assert-data empty-multi-graph dbl-branch-data)]
    (dbl-branch g)
    (dbl-branch gm)))

(defn dbl-branch-path [g]
  (let [r1 (resolve-pattern g '[:a ?p* :c])
        r2 (resolve-pattern g '[:a ?p* :f])
        r3 (resolve-pattern g '[:a ?p* :h])]
    (is (= [[[:p1 :p1]]] r1))
    (is (= [[[:p1 :p1 :p2]]] r2))
    (is (= [] r3))))

(deftest test-dbl-branch-path
  (let [g (assert-data empty-graph dbl-branch-data)
        gm (assert-data empty-multi-graph dbl-branch-data)]
    (dbl-branch-path g)
    (dbl-branch-path gm)))

(def simple-loop-data
  [[:a :p1 :b]
   [:b :p1 :c]
   [:c :p1 :a]])

(defn data-loop [g]
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

(deftest test-loop
  (let [g (assert-data empty-graph simple-loop-data)
        gm (assert-data empty-multi-graph simple-loop-data)]
    (data-loop g)
    (data-loop gm)))

(defn loop-path [g]
  (let [r1 (resolve-pattern g '[:a ?p* :c])
        r2 (resolve-pattern g '[:a ?p* :a])
        r3 (resolve-pattern g '[:a ?p+ :a])]
    (is (= [[[:p1 :p1]]] r1))
    (is (= [[[]]] r2))
    (is (= [[[:p1 :p1 :p1]]] r3))))

(deftest test-loop-path
  (let [g (assert-data empty-graph simple-loop-data)
        gm (assert-data empty-multi-graph simple-loop-data)]
    (loop-path g)
    (loop-path gm)))

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

(defn dbl-branch-loop [g]
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

(deftest test-dbl-branch-loop
  (let [g (assert-data empty-graph dbl-branch-loop-data)
        gm (assert-data empty-multi-graph dbl-branch-loop-data)]
    (dbl-branch-loop g)
    (dbl-branch-loop gm)))

(defn loop-variants [g]
  (let [g1 (assert-data g (remove #(= '[:b :p1 :c] %) dbl-branch-loop-data))
        r1 (unordered-resolve g1 '[:b ?p* ?x])
        g2 (assert-data g (remove #(= '[:d :p1 :a] %) dbl-branch-loop-data))
        r2 (unordered-resolve g2 '[:b ?p* ?x])
        g3 (assert-data g (remove #(= '[:b :p2 :c] %) dbl-branch-loop-data))
        r3 (unordered-resolve g3 '[:b ?p* ?x])]
    (is (= #{[:p1 :c] [:p2 :c] [:p1 :d] [:p2 :d] [:p1 :e] [:p2 :e]
             [:p1 :b] [:p2 :b] [:p1 :f] [:p2 :f] [:p1 :a] [:p2 :a]} r1))
    (is (= #{[:p1 :c] [:p2 :c] [:p1 :d] [:p2 :d] [:p1 :e] [:p2 :e]
             [:p1 :b] [:p2 :b] [:p1 :f] [:p2 :f]} r2))
    (is (= #{[:p1 :c] [:p1 :d] [:p1 :e]
             [:p1 :b] [:p1 :f] [:p1 :a]} r3))))

(deftest test-loop-variants
  (loop-variants empty-graph)
  (loop-variants empty-multi-graph))

(defn dbl-branch-loop-path [g]
  (let [r1 (resolve-pattern g '[:a ?p* :c])
        r2 (resolve-pattern g '[:a ?p* :f])
        r3 (resolve-pattern g '[:a ?p* :h])]
    (is (= [[[:p1 :p1]]] r1))
    (is (= [[[:p1 :p1 :p2]]] r2))
    (is (= [] r3))))

(deftest test-dbl-branch-loop-path
  (let [g (assert-data empty-graph dbl-branch-loop-data)
        gm (assert-data empty-multi-graph dbl-branch-loop-data)]
    (dbl-branch-loop-path g)
    (dbl-branch-loop-path gm)))


(def regions
  [[:columboola   :n :goombi]
   [:greenswamp   :n :goombi]
   [:columboola   :n :nangram]
   [:goombi       :n :rywung]
   [:greenswamp   :n :baking-board]
   [:rywung       :n :baking-board]
   [:columboola   :n :greenswamp]
   [:nangram      :n :greenswamp]
   [:greenswamp   :n :chinchilla]
   [:baking-board :n :chinchilla]
   [:nangram      :n :crossroads]
   [:greenswamp   :n :crossroads]
   [:crossroads   :n :hopeland]
   [:chinchilla   :n :boonarga]
   [:boonarga     :n :brigalow]
   [:hopeland     :n :brigalow]
   [:kogan        :n :brigalow]
   [:hopeland     :n :kogan]])

(def closure
  [[:columboola   :rywung]
   [:columboola   :baking-board]
   [:columboola   :chinchilla]
   [:columboola   :boonarga]
   [:columboola   :brigalow]
   [:columboola   :crossroads]
   [:columboola   :hopeland]
   [:columboola   :brigalow]
   [:columboola   :kogan]
   [:greenswamp   :rywung]
   [:greenswamp   :boonarga]
   [:greenswamp   :brigalow]
   [:greenswamp   :hopeland]
   [:greenswamp   :kogan]
   [:goombi       :baking-board]
   [:goombi       :chinchilla]
   [:goombi       :boonarga]
   [:goombi       :brigalow]
   [:rywung       :chinchilla]
   [:rywung       :boonarga]
   [:rywung       :brigalow]
   [:nangram      :goombi]
   [:nangram      :rywung]
   [:nangram      :baking-board]
   [:nangram      :chinchilla]
   [:nangram      :boonarga]
   [:nangram      :brigalow]
   [:nangram      :hopeland]
   [:nangram      :kogan]
   [:baking-board :chinchilla]
   [:baking-board :boonarga]
   [:baking-board :brigalow]
   [:crossroads   :brigalow]
   [:crossroads   :kogan]
   [:chinchilla   :brigalow]])

(defn rpairs
  [m]
  (for [[k vs] m v vs]
    [v k]))

(def spairs (comp set rpairs))

(deftest test-network-closure
  (let [g (assert-data empty-graph regions)
        non-trans (unordered-resolve g '[?a :n ?b])
        step1 (step-by-predicate (:n (:pos g)))
        step2 (step-by-predicate step1)
        step3 (step-by-predicate step2)
        r1 (unordered-resolve g '[?a :n+ ?b])
        r2 (unordered-resolve g '[?a :n* ?b])
        locations (-> #{} (into (map first regions)) (into (map #(nth % 2) regions)))
        zeros (map (fn [a] [a a]) locations)]
    (is (= non-trans (set (map (fn [[a _ b]] [a b]) regions))))
    (is (= non-trans (spairs (:n (:pos g)))))
    
    (is (every? (spairs step1) non-trans))
    (is (every? (spairs step2) (spairs step1)))
    (is (every? (spairs step2) (spairs step3)))
    (is (= (spairs step3) r1))
    (is (= r1 (into non-trans closure)))
    (is (= r2 (-> non-trans (into closure) (into zeros))))))

#?(:cljs (run-tests))
