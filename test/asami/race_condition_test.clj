(ns asami.race-condition-test
  (:require [asami.core :as asami]
            [clojure.test :as t]))

(t/deftest race-condition-test
  (let [db (asami/connect (str "asami:mem://" (gensym)))
        numbers (mapv (fn [n] {:n n}) (range 100))]
    (run!
     (fn [tx-data]
       (asami/transact db {:tx-data tx-data}))
     (partition 10 numbers))

    (Thread/sleep 1000)

    (t/is (= 100
             (count (asami/q '{:find [?n], :where [[_ :n ?n]]} (asami/db db)))))))
