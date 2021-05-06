(ns ^{:doc "Testing the block graph integration"
      :author "Paula Gearon"}
    asami.durable.graph-test
  (:require [asami.graph :refer [resolve-triple graph-transact new-node]]
            [asami.durable.graph :refer [graph-at new-block-graph]]
            [asami.durable.store :refer [unpack-tx tx-record-size]]
            [asami.durable.common :refer [latest close get-tx-data commit!]]
            #?(:clj [asami.durable.flat-file :as flat-file])
            [asami.durable.test-utils :as util :include-macros true]
            #?(:clj [clojure.java.io :as io])
            [clojure.test #?(:clj :refer :cljs :refer-macros) [is use-fixtures]]
            #?(:clj  [schema.test :as st :refer [deftest]]
               :cljs [schema.test :as st :refer-macros [deftest]])))

(use-fixtures :once st/validate-schemas)

(defn group-exists
  "Checks if a group of a given name exists"
  [n]
  #?(:clj (.isDirectory (io/file n)) :cljs false))

(defn remove-group
  "Removes the group resource"
  [n]
  #?(:clj (do
            (.delete (io/file n "tx.dat"))
            (.delete (io/file n)))
     :cljs true))

(defn make-graph
  "Creates a graph with a group name"
  [nm]
  (let [tx-manager #?(:clj (flat-file/tx-store nm "tx.dat" tx-record-size)
                      :cljs nil)
        tx (latest tx-manager)
        counter (atom 0)
        node-allocator (fn [] (new-node (swap! counter inc)))
        id-checker (fn [id] (when (> id @counter) (reset! counter id)))]
    (assoc (new-block-graph nm (and tx (unpack-tx tx)) node-allocator id-checker)
           :to-close tx-manager)))

(deftest test-new-graph
  (let [n "testdata-new"]
    (util/with-cleanup [block-graph (make-graph n)]
      (is (group-exists n)))
    (is (remove-group n))))

(def demo-data
  [[:a :name "Persephone Konstantopoulos"]
   [:a :age 23]
   [:a :friend :b]
   [:b :name "Anastasia Christodoulopoulos"]
   [:b :age 23]
   [:c :name "Anne Richardson"]
   [:c :age 25]])

(deftest simple-data
  (util/with-cleanup [graph (make-graph "testdata-simple")]
    (let [graph (graph-transact graph 0 demo-data nil)
          resolve (fn [[e a v]] (set (resolve-triple graph e a v)))]
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
      (is (= (set demo-data) (resolve '[?e ?a ?v])))))
  (is (remove-group "testdata-simple")))

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

(deftest phased-data
  (util/with-cleanup [graph (make-graph "testdata-phased")]
    (let [graph (commit! (graph-transact graph 0 demo-data nil))
          resolve (fn [[e a v]] (set (resolve-triple graph e a v)))
          tx (get-tx-data graph)]
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

      (let [g2 (commit! (graph-transact graph 1 update-data remove-data))
            r2 (fn [[e a v]] (set (resolve-triple g2 e a v)))
            tx2 (get-tx-data g2)
            g1 (graph-at g2 tx)
            r1 (fn [[e a v]] (set (resolve-triple g1 e a v)))]
        (is (not= (:r-spot tx) (:r-spot tx2)))
        (is (not= (:r-post tx) (:r-post tx2)))
        (is (not= (:r-ospt tx) (:r-ospt tx2)))
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


        (let [g3 (commit! (graph-transact g2 1 update-data2 remove-data2))
              r3 (fn [[e a v]] (set (resolve-triple g3 e a v)))
              tx3 (get-tx-data g3)
              g1 (graph-at g3 tx)
              g2 (graph-at g3 tx2)
              r1 (fn [[e a v]] (set (resolve-triple g1 e a v)))
              r2 (fn [[e a v]] (set (resolve-triple g2 e a v)))]
          (is (not= (:r-spot tx) (:r-spot tx2)))
          (is (not= (:r-spot tx) (:r-spot tx3)))
          (is (not= (:r-spot tx2) (:r-spot tx3)))
          (is (not= (:r-post tx) (:r-post tx2)))
          (is (not= (:r-post tx) (:r-post tx3)))
          (is (not= (:r-post tx2) (:r-post tx3)))
          (is (not= (:r-ospt tx) (:r-ospt tx2)))
          (is (not= (:r-ospt tx) (:r-ospt tx3)))
          (is (not= (:r-ospt tx2) (:r-ospt tx3)))
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
          (is (= (set demo-data) (r1 '[?e ?a ?v])))))))
  (is (remove-group "testdata-phased")))
