(ns asami.test-api
  "Tests the public query functionality"
  (:require [asami.core :refer [q connect db transact]]
            [asami.index :as i]
            [asami.multi-graph :as m]
            [schema.core :as s]
            #?(:clj  [clojure.test :refer [is use-fixtures testing]]
               :cljs [clojure.test :refer-macros [is run-tests use-fixtures testing]])
            #?(:clj  [schema.test :as st :refer [deftest]]
               :cljs [schema.test :as st :refer-macros [deftest]]))
  #?(:clj (:import [clojure.lang ExceptionInfo])))


(deftest test-connect
  (let [c (connect "asami:mem://apple")
        cm (connect "asami:multi://banana")]
    (is (instance? asami.index.GraphIndexed (:graph (:db @(:state c)))))
    (is (= "apple" (:name c)))
    (is (instance? asami.multi_graph.MultiGraph (:graph (:db @(:state cm)))))
    (is (= "banana" (:name cm)))))

(deftest load-data
  (let [c (connect "asami:mem://test")
        r (transact c {:tx-data [{:db/id "bobid"
                                  :person/name "Bob"
                                  :person/spouse "aliceid"}
                                 {:db/id "aliceid"
                                  :person/name "Alice"
                                  :person/spouse "bobid"}]})]
    (prn @r)
    )
  )
