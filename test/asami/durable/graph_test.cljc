(ns ^{:doc "Testing the block graph integration"
      :author "Paula Gearon"}
    asami.durable.graph-test
  (:require [asami.durable.graph :refer [graph-at new-block-graph]]
            [asami.durable.store :refer [unpack-tx tx-record-size]]
            [asami.durable.common :refer [latest close]]
            [clojure.test #?(:clj :refer :cljs :refer-macros) [deftest is]]
            #?(:clj [asami.durable.flat-file :as flat-file])))

(defn make-graph
  [nm]
  (let [tx-manager #?(:clj (flat-file/tx-store nm "tx.dat" tx-record-size) :cljc nil)
        tx (latest tx-manager)]
    (new-block-graph nm (unpack-tx tx))))

(deftest test-new-graph
  (let [block-graph (make-graph "testdata")]
    (close block-graph)))
