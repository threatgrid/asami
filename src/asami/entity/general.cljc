(ns ^{:doc "Common functionality for the entity reader/writer namespaces"
      :author "Paula Gearon"}
    asami.entity.general
  (:require [schema.core :as s :refer [=>]]
            [clojure.string :as string]
            #?(:clj [clojure.java.io :as io])
            #?(:clj [cheshire.core :as j])
            [naga.store :as store :refer [StorageType]]))

(def KeyValue [(s/one s/Keyword "Key") (s/one s/Any "Value")])

(def EntityMap {s/Keyword s/Any})



