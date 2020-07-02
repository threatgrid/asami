(ns asami.test-api
  "Tests the public query functionality"
  (:require [asami.core :refer [q create-database connect db transact]]
            [asami.index :as i]
            [asami.multi-graph :as m]
            [schema.core :as s]
            #?(:clj  [clojure.test :refer [is use-fixtures testing]]
               :cljs [clojure.test :refer-macros [is run-tests use-fixtures testing]])
            #?(:clj  [schema.test :as st :refer [deftest]]
               :cljs [schema.test :as st :refer-macros [deftest]]))
  #?(:clj (:import [clojure.lang ExceptionInfo])))

(deftest test-create
  (let [c1 (create-database "asami:mem://babaco")
        c2 (create-database "asami:mem://babaco")
        c3 (create-database "asami:mem://kumquat")
        c4 (create-database "asami:mem://kumquat")
        c5 (create-database "asami:multi://kumquat")]
    (is c1)
    (is (not c2))
    (is c3)
    (is (not c4))
    (is c5)
    (is (thrown-with-msg? ExceptionInfo #"Local Databases not yet implemented"
                          (create-database "asami:local://kumquat")))))

(deftest test-connect
  (let [c (connect "asami:mem://apple")
        cm (connect "asami:multi://banana")]
    (is (instance? asami.index.GraphIndexed (:graph (:db @(:state c)))))
    (is (= "apple" (:name c)))
    (is (instance? asami.multi_graph.MultiGraph (:graph (:db @(:state cm)))))
    (is (= "banana" (:name cm)))))

(deftest load-data
  (let [c (connect "asami:mem://test1")
        r (transact c {:tx-data [{:db/id "bobid"
                                  :person/name "Bob"
                                  :person/spouse "aliceid"}
                                 {:db/id "aliceid"
                                  :person/name "Alice"
                                  :person/spouse "bobid"}]})]
    (prn (:tx-data @r)))

  (let [c (connect "asami:mem://test2")
        r (transact c {:tx-data [[:db/add :mem/node-1 :property "value"]
                                 [:db/add :mem/node-2 :property "other"]]})]

    (is (= 2 (count (:tx-data @r)))))

  (let [c (connect "asami:mem://test2")
        maksim {:db/id -1
                :name  "Maksim"
                :age   45
                :wife {:db/id -2}
                :aka   ["Maks Otto von Stirlitz", "Jack Ryan"]}
        anna   {:db/id -2
                :name  "Anna"
                :age   31
                :husband {:db/id -1}
                :aka   ["Anitzka"]}
        r (transact c [maksim anna])]
    ;(prn @r)
    )
  )
