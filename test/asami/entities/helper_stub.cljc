(ns asami.entities.helper-stub
  (:require [zuko.node :as node]
            #?(:clj  [schema.core :as s]
               :cljs [schema.core :as s :include-macros true])))

(declare ->TestGraph)

(s/defrecord TestGraph [data n]
  node/NodeAPI
  (data-attribute [store data] :tg/first)
  (container-attribute [store data] :tg/contains)
  (new-node [store]
    (let [v (swap! n inc)]
      (keyword "test" (str "n" v))))
  (node-id [store n] (subs (name n) 1))
  (node-type? [store p n] (and (keyword? n)
                               (= "test" (namespace n))
                               (= \n (first (name n)))))
  (find-triple [store pattern] data))

(defn new-graph [] (->TestGraph [0] (atom 0)))

(def empty-graph (new-graph))
