(ns asami.test-api
  "Tests the public query functionality"
  (:require [asami.core :refer [q create-database connect db transact entity now as-of since]]
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
        r (transact c {:tx-data [{:db/ident "bobid"
                                  :person/name "Bob"
                                  :person/spouse "aliceid"}
                                 {:db/ident "aliceid"
                                  :person/name "Alice"
                                  :person/spouse "bobid"}]})]
    (= (->> (:tx-data @r)
            (filter #(= :db/ident (second %)))
            (map #(nth % 2))
            set)
       #{"bobid" "aliceid"}))

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
        {:keys [tempids tx-data] :as r} @(transact c [maksim anna])
        one (tempids -1)
        two (tempids -2)]
    (is (= 19 (count tx-data)))
    (is (= #{[one :db/ident one]
             [one :tg/entity true]
             [one :name "Maksim"]
             [one :age 45]
             [one :wife two]}
           (->> tx-data
                (filter #(= one (first %)))
                (remove #(= :aka (second %)))
                (map (partial take 3))
                set)))
    (is (= #{[two :db/ident two]
             [two :tg/entity true]
             [two :name "Anna"]
             [two :age 31]
             [two :husband one]}
           (->> tx-data
                (filter #(= two (first %)))
                (remove #(= :aka (second %)))
                (map (partial take 3))
                set))))

  (let [c (connect "asami:mem://test3")
        maksim {:db/ident "maksim"
                :name  "Maksim"
                :age   45
                :wife {:db/ident "anna"}
                :aka   ["Maks Otto von Stirlitz", "Jack Ryan"]}
        anna   {:db/ident "anna"
                :name  "Anna"
                :age   31
                :husband {:db/ident "maksim"}
                :aka   ["Anitzka"]}
        {:keys [tempids tx-data] :as r} @(transact c [maksim anna])
        one (tempids "maksim")
        two (tempids "anna")]
    (is (= 17 (count tx-data)))
    (is (= #{[one :db/ident "maksim"]
             [one :name "Maksim"]
             [one :age 45]
             [one :wife two]}
           (->> tx-data
                (filter #(= one (first %)))
                (remove #(= :aka (second %)))
                (map (partial take 3))
                set)))
    (is (= #{[two :db/ident "anna"]
             [two :name "Anna"]
             [two :age 31]
             [two :husband one]}
           (->> tx-data
                (filter #(= two (first %)))
                (remove #(= :aka (second %)))
                (map (partial take 3))
                set)))))

(deftest test-entity
  (let [c (connect "asami:mem://test4")
        maksim {:db/id -1
                :db/ident :maksim
                :name  "Maksim"
                :age   45
                :aka   ["Maks Otto von Stirlitz", "Jack Ryan"]}
        anna   {:db/id -2
                :db/ident :anna
                :name  "Anna"
                :age   31
                :husband {:db/id -1}
                :aka   ["Anitzka"]}
        {:keys [tempids tx-data] :as r} @(transact c [maksim anna])
        one (tempids -1)
        two (tempids -2)
        d (db c)]
    (is (= {:name  "Maksim"
            :age   45
            :aka   ["Maks Otto von Stirlitz", "Jack Ryan"]}
           (entity d one)))
    (is (= {:name  "Maksim"
            :age   45
            :aka   ["Maks Otto von Stirlitz", "Jack Ryan"]}
           (entity d :maksim)))
    (is (= {:name  "Anna"
            :age   31
            :husband {:name  "Maksim"
                      :age   45
                      :aka   ["Maks Otto von Stirlitz", "Jack Ryan"]}
            :aka   ["Anitzka"]}
           (entity d :anna)))))

(defn sleep [msec]
  #?(:clj
     (Thread/sleep msec)
     :cljs
     (let [deadline (+ msec (.getTime (js/Date.)))]
       (while (> deadline (.getTime (js/Date.)))))))

(deftest test-as
  (let [t0 (now)
        _ (sleep 100)
        c (connect "asami:mem://test5")
        _ (sleep 100)
        t1 (now)
        maksim {:db/id -1
                :db/ident :maksim
                :name  "Maksim"
                :age   45
                :aka   ["Maks Otto von Stirlitz", "Jack Ryan"]}
        anna   {:db/id -2
                :db/ident :anna
                :name  "Anna"
                :age   31
                :husband {:db/ident :maksim}
                :aka   ["Anitzka"]}
        _ (sleep 100)
        _ @(transact c [maksim])
        _ (sleep 100)
        t2 (now)
        _ (sleep 100)
        _ @(transact c [anna])
        _ (sleep 100)
        t3 (now)
        latest-db (db c)
        db0 (as-of latest-db t0)  ;; before
        db1 (as-of latest-db t1)  ;; empty
        db2 (as-of latest-db t2)  ;; after first tx
        db3 (as-of latest-db t3)  ;; after second tx
        db0' (as-of latest-db 0)  ;; empty
        db1' (as-of latest-db 1)  ;; after first tx
        db2' (as-of latest-db 2)  ;; after second tx
        db3' (as-of latest-db 3)  ;; still after second tx

        db0* (since latest-db t0)  ;; before
        db1* (since latest-db t1)  ;; after first tx
        db2* (since latest-db t2)  ;; after second tx
        db3* (since latest-db t3)  ;; well after second tx
        db0*' (since latest-db 0)  ;; after first tx
        db1*' (since latest-db 1)  ;; after second tx 
        db2*' (since latest-db 2)  ;; still after second tx
        db3*' (since latest-db 3)] ;; still after second tx
    (is (= db0 db0'))
    (is (= db0 db1))
    (is (= db2 db1'))
    (is (= db3 db2'))
    (is (= db2' db3'))
    (is (= db0 db0*))
    (is (= db2 db1*))
    (is (= db3 db2*))
    (is (= nil db3*))
    (is (= db2 db0*'))
    (is (= db3 db1*'))
    (is (= nil db2*'))
    (is (= nil db3*'))
    (is (= db3 latest-db))
    (is (= (set (q '[:find ?name :where [?e :name ?name]] db0))
           #{}))
    (is (= (set (q '[:find ?name :where [?e :name ?name]] db1))
           #{}))
    (is (= (set (q '[:find ?name :where [?e :name ?name]] db2))
           #{["Maksim"]}))
    (is (= (set (q '[:find ?name :where [?e :name ?name]] db3))
           #{["Maksim"] ["Anna"]}))))

(deftest test-update
  (let [c (connect "asami:mem://test6")
        maksim {:db/id -1
                :db/ident :maksim
                :name  "Maksim"
                :age   45
                :aka   ["Maks Otto von Stirlitz", "Jack Ryan"]}
        anna   {:db/id -2
                :db/ident :anna
                :name  "Anna"
                :age   31
                :husband {:db/id -1}
                :aka   ["Anitzka"]}
        {db1 :db-after} @(transact c [maksim anna])
        anne   {:db/ident :anna
                :name' "Anne"
                :age   32}
        {db2 :db-after} @(transact c [anne])
        maks   {:db/ident :maksim
                :aka' ["Maks Otto von Stirlitz"]}
        {db3 :db-after} @(transact c [maks])]
    (is (= (set (q '[:find ?aka :where [?e :name "Maksim"] [?e :aka ?a] [?a :tg/contains ?aka]] db1))
           #{["Maks Otto von Stirlitz"] ["Jack Ryan"]}))
    (is (= (set (q '[:find ?age :where [?e :db/ident :anna] [?e :age ?age]] db1))
           #{[31]}))
    (is (= (set (q '[:find ?name :where [?e :db/ident :anna] [?e :name ?name]] db1))
           #{["Anna"]}))
    (is (= (set (q '[:find ?age :where [?e :db/ident :anna] [?e :age ?age]] db2))
           #{[31] [32]}))
    (is (= (set (q '[:find ?name :where [?e :db/ident :anna] [?e :name ?name]] db2))
           #{["Anne"]}))
    (is (= (set (q '[:find ?aka :where [?e :name "Maksim"] [?e :aka ?a] [?a :tg/contains ?aka]] db3))
           #{["Maks Otto von Stirlitz"]}))))

#?(:cljs (run-tests))
