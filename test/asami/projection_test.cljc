(ns asami.projection-test
  (:require [asami.projection :as projection]
            #?(:clj [clojure.test :as t]
               :cljs [cljs.test :as t :include-macros true]))
  #?(:clj (:import [clojure.lang ExceptionInfo])))

(t/deftest test-project-tuple
  (let [tuple '[?a ?b ?c]
        columns '[?q ?t ?c ?b ?z ?a]
        data [[1 2 3 4 5 6] [7 8 9 10 11 12]]]
    (t/is (= [6 4 3] (projection/project-tuple tuple columns data)))
    (t/is (= nil (projection/project-tuple tuple columns nil)))
    (t/is (= nil (projection/project-tuple tuple columns '())))
    (t/is (thrown-with-msg? ExceptionInfo #"Projection variables not found in the selected data: \[\?a\]"
                          (projection/project-tuple tuple '[?q ?t ?c ?b ?z ?d] data)))

    (let [mdata (with-meta data {:cols columns})]
      (t/is (= [6 4 3] (projection/project {} [tuple] mdata)))
      (t/is (= [3] (projection/project {} '[[?c]] mdata)))
      (t/is (= nil (projection/project {} '[[?c]] (with-meta '() {:cols columns})))))))

(t/deftest test-project-single
  (let [columns '[?q ?t ?c ?b ?z ?a]
        data [[1 2 3 4 5 6] [7 8 9 10 11 12]]]
    (t/is (= 3 (projection/project-single '?c columns data)))
    (t/is (thrown-with-msg? ExceptionInfo #"Projection variable was not in the selected data: \?c"
                            (projection/project-single '?c '[?q ?t ?a ?b ?z ?d] data)))

    (let [mdata (with-meta data {:cols columns})]
      (t/is (= 3 (projection/project {} '[?c .] mdata))))))

(t/deftest test-project-collection
  (let [columns '[?q ?t ?c ?b ?z ?a]
        data [[1 2 3 4 5 6] [7 8 9 10 11 12]]]
    (t/is (= [3 9] (projection/project-collection '?c columns data)))
    (t/is (thrown-with-msg? ExceptionInfo #"Projection variable was not in the selected data: \?c"
                          (projection/project-single '?c '[?q ?t ?a ?b ?z ?d] data)))

    (let [mdata (with-meta data {:cols columns})]
      (t/is (= [3 9]
               (projection/project {} '[[?c ...]] mdata))))))

(t/deftest test-project-results
  (let [selection '[?a ?b ?c]
        columns '[?q ?t ?c ?b ?z ?a]
        data [[1 2 3 4 5 6] [7 8 9 10 11 12]]]
    (t/is (= [[6 4 3] [12 10 9]] (projection/project-results {} selection columns data)))

    (let [mdata (with-meta data {:cols columns})]
      (t/is (= [[6 4 3] [12 10 9]] (projection/project {} selection mdata))))))


#?(:cljs (run-tests))
