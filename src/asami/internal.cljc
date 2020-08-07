(ns ^{:doc "Common internal elements of storage"
      :author "Paula Gearon"}
    asami.internal
  (:require [asami.graph :as graph]))


(def project-args {:new-node graph/new-node
                   :node-label graph/node-label})

(defn project-ins-args
  [graph]
  (assoc project-args
         :resolve-pattern (partial graph/resolve-pattern graph)))

