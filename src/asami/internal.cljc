(ns ^{:doc "Common internal elements of storage"
      :author "Paula Gearon"}
    asami.internal
  (:require [schema.core :as s :refer [=>]]
            [clojure.string :as string]
            #?(:cljs [cljs.core :refer [Symbol]])
            [asami.graph :as graph])
  #?(:clj (:import [clojure.lang Symbol])))



(def project-args {:new-node graph/new-node
                   :node-label graph/node-label})

(defn project-ins-args
  [graph]
  (assoc project-args
         :resolve-pattern (partial graph/resolve-pattern graph)))

