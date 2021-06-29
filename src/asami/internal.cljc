(ns ^{:doc "Common internal elements of storage"
      :author "Paula Gearon"}
    asami.internal
  (:require [asami.graph :as graph]
            [asami.cache :refer [lookup hit miss lru-cache-factory]])
  #?(:clj (:import [java.util Date])))

#?(:clj (set! *warn-on-reflection* true))

(defn now
  "Creates an object to represent the current time"
  []
  #?(:clj (Date.)
     :cljs (js/Date.)))

(defn instant?
  "Tests if a value is a timestamp"
  [t]
  (= #?(:clj Date :cljs js/Date) (type t)))

(defn instant
  "Creates an instant from a long millisecond value"
  [^long ms]
  #?(:clj (Date. ms) :cljs (js/Date. ms)))

(defn long-time
  "Converts a timestamp to a long value as the number of milliseconds"
  #?(:clj [^java.util.Date t]
     :cljs [t])
  (.getTime t))  ;; this is an identical operation in both Java and Javascript

(def project-args {:new-node graph/new-node
                   :node-label graph/node-label})

(defn project-ins-args
  [graph]
  (assoc project-args
         :resolve-pattern (partial graph/resolve-pattern graph)))

(defn shallow-cache-1
  "Builds a cached version of an arity-1 function that contains only a small number of cached items.
  size: the number of results to cache.
  f: The arity-1 function to cache results for."
  [size f]
  (let [cache (atom (lru-cache-factory {} :threshold size))]
    (fn [arg]
      (if-let [ret (lookup @cache arg)]
        (do
          (swap! cache hit arg)
          ret)
        (let [ret (f arg)]
          (swap! cache miss arg ret)
          ret)))))
