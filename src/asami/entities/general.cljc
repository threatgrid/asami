(ns ^{:doc "Common functionality for the entity reader/writer namespaces"
      :author "Paula Gearon"}
    asami.entities.general
  (:require [schema.core :as s :refer [=>]]
            [clojure.string :as string]
            [zuko.node :as node]
            [naga.store :as store :refer [StorageType]]))

(def tg-ns "tg")

(def KeyValue [(s/one s/Any "Key") (s/one s/Any "Value")])

(def EntityMap {s/Any s/Any})


(def Result [(s/one s/Any "first") (s/optional s/Any "second") (s/optional s/Any "third")])
(def Pattern [(s/one s/Any "entity") (s/one s/Any "attribute") (s/one s/Any "value")])

;; The resolver function takes a single pattern argument, and returns a seq of Result
(def ResolverFn (=> [Result] [Pattern]))

(def GraphType (s/pred #(satisfies? node/NodeAPI %)))


