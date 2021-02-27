(ns ^{:doc "Testing the block graph integration"
      :author "Paula Gearon"}
    asami.durable.graph-test
  (:require [asami.graph :refer [resolve-triple graph-transact]]
            [asami.durable.graph :refer [graph-at new-block-graph]]
            [asami.durable.store :refer [unpack-tx tx-record-size]]
            [asami.durable.common :refer [latest close]]
            #?(:clj [asami.durable.flat-file :as flat-file])
            [asami.durable.test-utils :as util]
            [clojure.java.io :as io]
            [clojure.test #?(:clj :refer :cljs :refer-macros) [deftest is]]))

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
                      :cljc nil)
        tx (latest tx-manager)]
    (new-block-graph nm (unpack-tx tx))))

(deftest test-new-graph
  (let [n "testdata-new"]
    (util/with-cleanup [block-graph (make-graph n)]
      (is (group-exists n)))
    (is (remove-group n))))

(def demo-data
  [[:a :name "Mary"]
   [:a :age 23]
   [:a :friend :b]
   [:b :name "Jane"]
   [:b :age 23]
   [:c :name "Anne"]
   [:c :age 25]])

(deftest simple-data
  (util/with-cleanup [graph (make-graph "testdata-simple")]
    (let [graph (graph-transact graph 0 demo-data nil)
          resolve (fn [[e a v]] (set (resolve-triple graph e a v)))]
      (is (= #{[:name "Mary"]
               [:age 23]
               [:friend :b]}
             (resolve '[:a ?a ?v])))
      (is (= #{[:a "Mary"]
               [:b "Jane"]
               [:c "Anne"]}
             (resolve '[?e :name ?v])))
      (is (= #{[:a :age]
               [:b :age]}
             (resolve '[?e ?a 23])))
      (is (= #{["Jane"]}
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
