(ns asami.projection-test
  (:require [asami.projection :as projection]
            #?(:clj [clojure.test :as t]
               :cljs [cljs.test :as t :include-macros true])))

(t/deftest derive-pattern-and-columns-test
  (t/is (= '[[] []]
           (projection/derive-pattern-and-columns [])))

  (t/is (= '[[?x] [?x]]
           (projection/derive-pattern-and-columns '[?x])))

  ;; Dangling :as is ignored.
  (t/is (= '[[:data] [:data]]
           (projection/derive-pattern-and-columns '[:data :as])))

  (t/is (= '[[:data] [:data]]
           (projection/derive-pattern-and-columns '[:data])))

  (t/is (= '[[:data] [?x]]
           (projection/derive-pattern-and-columns '[:data :as ?x])))

  (t/is (= '[[?x :data ?y] [?x ?z ?y]]
           (projection/derive-pattern-and-columns '[?x :data :as ?z ?y]))))


