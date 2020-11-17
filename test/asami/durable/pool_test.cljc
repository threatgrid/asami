(ns ^{:doc "Tests the Data Pool"
      :author "Paula Gearon"}
    asami.durable.pool-test
  (:require [clojure.test :refer [deftest is]]
            [asami.durable.common :refer [close]]
            [asami.durable.pool :refer [create-pool]]))

(deftest test-creation
  (let [pool (create-pool "empty-test")]
    (close pool)))
