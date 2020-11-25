(ns ^{:doc "Common internal elements of storage"
      :author "Paula Gearon"}
    asami.internal
  (:require [asami.graph :as graph])
  #?(:clj (:import [java.util Date])))

(defn now
  "Creates an object to represent the current time"
  []
  #?(:clj (Date.)
     :cljs (js/Date.)))

(defn instant?
  "Tests if a value is a timestamp"
  [t]
  (= #?(:clj Date :cljs js/Date) (type t)))


(def project-args {:new-node graph/new-node
                   :node-label graph/node-label})

(defn project-ins-args
  [graph]
  (assoc project-args
         :resolve-pattern (partial graph/resolve-pattern graph)))

