(ns ^{:doc "Testing the Store API for durable graphs"
      :author "Paula Gearon"}
    asami.durable.store-test
  (:require [asami.durable.store :refer [exists? create-database]]
            [asami.storage :refer [db delete-database transact-data as-of as-of-t since since-t graph entity]]
            [asami.graph :refer [resolve-triple]]
            [asami.internal :refer [instant]]
            [asami.durable.common :as common]
            #?(:clj [clojure.java.io :as io])
            [clojure.test #?(:clj :refer :cljs :refer-macros) [deftest is]]))


(deftest test-create
  (let [dbname "empty-db"]
    (is (not (exists? dbname)))
    (let [c (create-database dbname)]
      (is (exists? dbname))
      (delete-database c))
    (is (not (exists? dbname)))))


(def demo-data
  [[:a :name "Persephone Konstantopoulos"]
   [:a :age 23]
   [:a :friend :b]
   [:b :name "Anastasia Christodoulopoulos"]
   [:b :age 23]
   [:c :name "Anne Richardson"]
   [:c :age 25]])

(deftest test-simple-data
  (let [dbname "simple-db"
        conn (create-database dbname)
        [_ db1] (transact-data conn demo-data nil)
        db2 (db conn)
        r1 (fn [[e a v]] (set (resolve-triple (graph db1) e a v)))
        r2 (fn [[e a v]] (set (resolve-triple (graph db2) e a v)))]
    (is (= #{[:name "Persephone Konstantopoulos"]
             [:age 23]
             [:friend :b]}
           (r1 '[:a ?a ?v])))
    (is (= #{[:name "Persephone Konstantopoulos"]
             [:age 23]
             [:friend :b]}
           (r2 '[:a ?a ?v])))
    (is (= #{[:a "Persephone Konstantopoulos"]
             [:b "Anastasia Christodoulopoulos"]
             [:c "Anne Richardson"]}
           (r1 '[?e :name ?v])))
    (is (= #{[:a "Persephone Konstantopoulos"]
             [:b "Anastasia Christodoulopoulos"]
             [:c "Anne Richardson"]}
           (r2 '[?e :name ?v])))
    (is (= #{[:a :age]
             [:b :age]}
           (r1 '[?e ?a 23])))
    (is (= #{[:a :age]
             [:b :age]}
           (r2 '[?e ?a 23])))
    (is (= #{["Anastasia Christodoulopoulos"]}
           (r1 '[:b :name ?v])))
    (is (= #{["Anastasia Christodoulopoulos"]}
           (r2 '[:b :name ?v])))
    (is (= #{[:friend]}
           (r1 '[:a ?a :b])))
    (is (= #{[:friend]}
           (r2 '[:a ?a :b])))
    (is (= #{[:a]
             [:b]}
           (r1 '[?e :age 23])))
    (is (= #{[:a]
             [:b]}
           (r2 '[?e :age 23])))
    (is (= #{[]}
           (r1 '[:b :age 23])))
    (is (= #{[]}
           (r2 '[:b :age 23])))
    (is (empty? (r1 '[:b :age 24])))
    (is (empty? (r2 '[:b :age 24])))
    (is (= (set demo-data) (r1 '[?e ?a ?v])))
    (is (= (set demo-data) (r2 '[?e ?a ?v])))
    (delete-database conn)))

(def update-data
  [[:a :age 24]
   [:b :age 24]
   [:c :age 26]])

(def remove-data
  [[:a :age 23]
   [:b :age 23]
   [:c :age 25]
   [:a :friend :b]])

(def update-data2
  [[:a :age 25]
   [:b :age 25]
   [:c :age 27]
   [:a :friend :c]
   [:a :name "Persephone Smith"]])

(def remove-data2
  [[:a :age 24]
   [:b :age 24]
   [:c :age 26]
   [:a :name "Persephone Konstantopoulos"]])

(deftest test-phased-data
  (let [dbname "ph-testdata-db"
        conn (create-database dbname)
        [_ db1] (transact-data conn demo-data nil)
        resolve (fn [[e a v]] (set (resolve-triple (graph db1) e a v)))
        t1 (:t db1)]
    (is (= #{[:name "Persephone Konstantopoulos"]
             [:age 23]
             [:friend :b]}
           (resolve '[:a ?a ?v])))
    (is (= #{[:a "Persephone Konstantopoulos"]
             [:b "Anastasia Christodoulopoulos"]
             [:c "Anne Richardson"]}
           (resolve '[?e :name ?v])))
    (is (= #{[:a :age]
             [:b :age]}
           (resolve '[?e ?a 23])))
    (is (= #{["Anastasia Christodoulopoulos"]}
           (resolve '[:b :name ?v])))
    (is (= #{[:friend]}
           (resolve '[:a ?a :b])))
    (is (= #{[:a]
             [:b]}
           (resolve '[?e :age 23])))
    (is (= #{[]}
           (resolve '[:b :age 23])))
    (is (empty? (resolve '[:b :age 24])))
    (is (= (set demo-data) (resolve '[?e ?a ?v])))

    (let [_ (transact-data conn update-data remove-data)
          db2 (db conn)
          ts2 (:timestamp db2)
          r2 (fn [[e a v]] (set (resolve-triple (graph db2) e a v)))
          db1 (as-of db2 t1)
          r1 (fn [[e a v]] (set (resolve-triple (graph db1) e a v)))]
      (is (= #{[:name "Persephone Konstantopoulos"]
               [:age 24]}
             (r2 '[:a ?a ?v])))
      (is (= #{[:a "Persephone Konstantopoulos"]
               [:b "Anastasia Christodoulopoulos"]
               [:c "Anne Richardson"]}
             (r2 '[?e :name ?v])))
      (is (= #{[:a :age]
               [:b :age]}
             (r2 '[?e ?a 24])))
      (is (empty? (r2 '[?e ?a 23])))
      (is (= #{["Anastasia Christodoulopoulos"]}
             (r2 '[:b :name ?v])))
      (is (empty? (r2 '[:a ?a :b])))
      (is (= #{[:a]
               [:b]}
             (r2 '[?e :age 24])))
      (is (empty? (r2 '[?e :age 23])))
      (is (= #{[]}
             (r2 '[:b :age 24])))
      (is (empty? (r2 '[:b :age 23])))
      (is (empty? (r2 '[:b :age 25])))
      (is (= (set (concat update-data (filter #(= :name (second %)) demo-data))) (r2 '[?e ?a ?v])))

      (is (= #{[:name "Persephone Konstantopoulos"]
               [:age 23]
               [:friend :b]}
             (r1 '[:a ?a ?v])))
      (is (= #{[:a "Persephone Konstantopoulos"]
               [:b "Anastasia Christodoulopoulos"]
               [:c "Anne Richardson"]}
             (r1 '[?e :name ?v])))
      (is (= #{[:a :age]
               [:b :age]}
             (r1 '[?e ?a 23])))
      (is (= #{["Anastasia Christodoulopoulos"]}
             (r1 '[:b :name ?v])))
      (is (= #{[:friend]}
             (r1 '[:a ?a :b])))
      (is (= #{[:a]
               [:b]}
             (r1 '[?e :age 23])))
      (is (= #{[]}
             (r1 '[:b :age 23])))
      (is (empty? (r1 '[:b :age 24])))
      (is (= (set demo-data) (r1 '[?e ?a ?v])))


      (let [_ (transact-data conn update-data2 remove-data2)
            db3 (db conn)
            r3 (fn [[e a v]] (set (resolve-triple (graph db3) e a v)))
            ts2-time-- (instant (dec ts2))
            db2 (since db3 ts2-time--)
            db1 (as-of db3 ts2-time--)
            r1 (fn [[e a v]] (set (resolve-triple (graph db1) e a v)))
            r2 (fn [[e a v]] (set (resolve-triple (graph db2) e a v)))]
        (is (= #{[:name "Persephone Smith"]
                 [:age 25]
                 [:friend :c]}
               (r3 '[:a ?a ?v])))
        (is (= #{[:a "Persephone Smith"]
                 [:b "Anastasia Christodoulopoulos"]
                 [:c "Anne Richardson"]}
               (r3 '[?e :name ?v])))
        (is (= #{[:a :age]
                 [:b :age]}
               (r3 '[?e ?a 25])))
        (is (empty? (r3 '[?e ?a 23])))
        (is (empty? (r3 '[?e ?a 24])))
        (is (= #{["Anastasia Christodoulopoulos"]}
               (r3 '[:b :name ?v])))
        (is (empty? (r3 '[:a ?a :b])))
        (is (= #{[:friend]}
               (r3 '[:a ?a :c])))
        (is (= #{[:a]
                 [:b]}
               (r3 '[?e :age 25])))
        (is (empty? (r3 '[?e :age 23])))
        (is (empty? (r3 '[?e :age 24])))
        (is (= #{[]}
               (r3 '[:b :age 25])))
        (is (empty? (r3 '[:b :age 23])))
        (is (empty? (r3 '[:b :age 24])))
        (is (empty? (r3 '[:b :age 26])))
        (is (= (set (concat update-data2 (remove #(= :a (first %)) (filter #(= :name (second %)) demo-data)))) (r3 '[?e ?a ?v])))

        (is (= #{[:name "Persephone Konstantopoulos"]
                 [:age 24]}
               (r2 '[:a ?a ?v])))
        (is (= #{[:a "Persephone Konstantopoulos"]
                 [:b "Anastasia Christodoulopoulos"]
                 [:c "Anne Richardson"]}
               (r2 '[?e :name ?v])))
        (is (= #{[:a :age]
                 [:b :age]}
               (r2 '[?e ?a 24])))
        (is (empty? (r2 '[?e ?a 23])))
        (is (= #{["Anastasia Christodoulopoulos"]}
               (r2 '[:b :name ?v])))
        (is (empty? (r2 '[:a ?a :b])))
        (is (= #{[:a]
                 [:b]}
               (r2 '[?e :age 24])))
        (is (empty? (r2 '[?e :age 23])))
        (is (= #{[]}
               (r2 '[:b :age 24])))
        (is (empty? (r2 '[:b :age 23])))
        (is (empty? (r2 '[:b :age 25])))
        (is (= (set (concat update-data (filter #(= :name (second %)) demo-data))) (r2 '[?e ?a ?v])))

        (is (= #{[:name "Persephone Konstantopoulos"]
                 [:age 23]
                 [:friend :b]}
               (r1 '[:a ?a ?v])))
        (is (= #{[:a "Persephone Konstantopoulos"]
                 [:b "Anastasia Christodoulopoulos"]
                 [:c "Anne Richardson"]}
               (r1 '[?e :name ?v])))
        (is (= #{[:a :age]
                 [:b :age]}
               (r1 '[?e ?a 23])))
        (is (= #{["Anastasia Christodoulopoulos"]}
               (r1 '[:b :name ?v])))
        (is (= #{[:friend]}
               (r1 '[:a ?a :b])))
        (is (= #{[:a]
                 [:b]}
               (r1 '[?e :age 23])))
        (is (= #{[]}
               (r1 '[:b :age 23])))
        (is (empty? (r1 '[:b :age 24])))
        (is (= (set demo-data) (r1 '[?e ?a ?v])))))
    (delete-database conn)))

(defn sleep
  "A horrible way to timeout in ClojureScript. Do not do this unless the timeout is small."
  [timeout]
  #?(:clj (Thread/sleep timeout)
     :cljs (let [maxtime (+ (.getTime (js/Date.)) timeout)]
             (while (< (.getTime (js/Date.)) maxtime)))))

(deftest test-saved-data
  (let [dbname "saved-testdata-db"
        conn (create-database dbname)
        _ (transact-data conn demo-data nil)
        _ (sleep 10)
        [_ db2] (transact-data conn update-data remove-data)
        ts2 (:timestamp db2)
        _ (transact-data conn update-data2 remove-data2)]

    (common/close @(:grapha conn))
    (common/close (:tx-manager conn))

    (is (exists? dbname))

    (let [conn (create-database dbname)
          db3 (db conn)
          r3 (fn [[e a v]] (set (resolve-triple (graph db3) e a v)))
          ts2-time-- (instant (dec ts2))
          db2 (since db3 ts2-time--)
          db1 (as-of db3 ts2-time--)
          r1 (fn [[e a v]] (set (resolve-triple (graph db1) e a v)))
          r2 (fn [[e a v]] (set (resolve-triple (graph db2) e a v)))]
      

      (is (= #{[:name "Persephone Smith"]
               [:age 25]
               [:friend :c]}
             (r3 '[:a ?a ?v])))
      (is (= #{[:a "Persephone Smith"]
               [:b "Anastasia Christodoulopoulos"]
               [:c "Anne Richardson"]}
             (r3 '[?e :name ?v])))
      (is (= #{[:a :age]
               [:b :age]}
             (r3 '[?e ?a 25])))
      (is (empty? (r3 '[?e ?a 23])))
      (is (empty? (r3 '[?e ?a 24])))
      (is (= #{["Anastasia Christodoulopoulos"]}
             (r3 '[:b :name ?v])))
      (is (empty? (r3 '[:a ?a :b])))
      (is (= #{[:friend]}
             (r3 '[:a ?a :c])))
      (is (= #{[:a]
               [:b]}
             (r3 '[?e :age 25])))
      (is (empty? (r3 '[?e :age 23])))
      (is (empty? (r3 '[?e :age 24])))
      (is (= #{[]}
             (r3 '[:b :age 25])))
      (is (empty? (r3 '[:b :age 23])))
      (is (empty? (r3 '[:b :age 24])))
      (is (empty? (r3 '[:b :age 26])))
      (is (= (set (concat update-data2 (remove #(= :a (first %)) (filter #(= :name (second %)) demo-data)))) (r3 '[?e ?a ?v])))

      (is (= #{[:name "Persephone Konstantopoulos"]
               [:age 24]}
             (r2 '[:a ?a ?v])))
      (is (= #{[:a "Persephone Konstantopoulos"]
               [:b "Anastasia Christodoulopoulos"]
               [:c "Anne Richardson"]}
             (r2 '[?e :name ?v])))
      (is (= #{[:a :age]
               [:b :age]}
             (r2 '[?e ?a 24])))
      (is (empty? (r2 '[?e ?a 23])))
      (is (= #{["Anastasia Christodoulopoulos"]}
             (r2 '[:b :name ?v])))
      (is (empty? (r2 '[:a ?a :b])))
      (is (= #{[:a]
               [:b]}
             (r2 '[?e :age 24])))
      
      (is (empty? (r2 '[?e :age 23])))
      (is (= #{[]}
             (r2 '[:b :age 24])))
      (is (empty? (r2 '[:b :age 23])))
      (is (empty? (r2 '[:b :age 25])))
      (is (= (set (concat update-data (filter #(= :name (second %)) demo-data))) (r2 '[?e ?a ?v])))

      (is (= #{[:name "Persephone Konstantopoulos"]
               [:age 23]
               [:friend :b]}
             (r1 '[:a ?a ?v])))
      (is (= #{[:a "Persephone Konstantopoulos"]
               [:b "Anastasia Christodoulopoulos"]
               [:c "Anne Richardson"]}
             (r1 '[?e :name ?v])))
      (is (= #{[:a :age]
               [:b :age]}
             (r1 '[?e ?a 23])))
      (is (= #{["Anastasia Christodoulopoulos"]}
             (r1 '[:b :name ?v])))
      (is (= #{[:friend]}
             (r1 '[:a ?a :b])))
      (is (= #{[:a]
               [:b]}
             (r1 '[?e :age 23])))
      
      (is (= #{[]}
             (r1 '[:b :age 23])))
      (is (empty? (r1 '[:b :age 24])))
      (is (= (set demo-data) (r1 '[?e ?a ?v])))
      (delete-database conn))))


(def entity-data
  [[:a :db/ident "p"]
   [:a :tg/entity true]
   [:a :name "Persephone Konstantopoulos"]
   [:a :age 23]
   [:a :friends :tg/node-b]
   [:tg/node-b :tg/first :tg/node-1]
   [:tg/node-b :tg/rest :tg/node-c]
   [:tg/node-c :tg/first :tg/node-2]
   [:tg/node-1 :name "Anastasia Christodoulopoulos"]
   [:tg/node-1 :age 23]
   [:tg/node-2 :name "Anne Richardson"]
   [:tg/node-2 :age 25]
   [:tg/node-b :tg/contains :tg/node-1]
   [:tg/node-b :tg/contains :tg/node-2]])

(deftest test-entity-data
  (let [dbname "entity-db"
        conn (create-database dbname)
        [_ db1] (transact-data conn entity-data nil)]
    (is (= {:name "Persephone Konstantopoulos"
            :age 23
            :friends [{:name "Anastasia Christodoulopoulos" :age 23}
                      {:name "Anne Richardson" :age 25}]}
         (entity db1 :a)))
    (is (= {:name "Persephone Konstantopoulos"
            :age 23
            :friends [{:name "Anastasia Christodoulopoulos" :age 23}
                      {:name "Anne Richardson" :age 25}]}
         (entity db1 "p")))
    (delete-database conn)))
