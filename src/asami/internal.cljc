(ns ^{:doc "Common internal elements of storage"
      :author "Paula Gearon"}
    asami.internal
  (:require [asami.graph :as graph]
            [asami.cache :refer [lookup hit miss lru-cache-factory]])
  #?(:clj (:import [java.util Date]
                   [java.time Instant])))

#?(:clj (set! *warn-on-reflection* true))

(defprotocol TimeType
  (instant? [this] "Indicates if this object is a time type that supports an instant")
  (long-time [this] "Returns a long value as the number of milliseconds since the epoch")
  (to-timestamp [this] "Converts to a common time type. Useful for comparison"))

(extend-protocol TimeType
  #?(:clj Date :cljs js/Date)
  (instant? [_] true)
  (long-time [this] (.getTime this))
  (to-timestamp [this] this)

  #?@(:clj
      [Instant
       (instant? [_] true)
       (long-time [this] (.toEpochMilli this))
       (to-timestamp [this] (Date. (.toEpochMilli this)))])

  #?(:clj Object :cljs default)
  (instant? [_] false)
  (long-time [this] (throw (ex-info (str "Unable to convert " (type this) " to a time") {:object this})))
  (to-timestamp [this] (throw (ex-info (str "Unable to convert " (type this) " to a time") {:object this}))))

(defn now
  "Creates an object to represent the current time"
  []
  #?(:clj (Date.)
     :cljs (js/Date.)))

(defn instant
  "Creates an instant from a long millisecond value"
  [^long ms]
  #?(:clj (Date. ms) :cljs (js/Date. ms)))

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
