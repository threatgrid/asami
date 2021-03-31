(ns ^{:doc "Common internal elements of storage"
      :author "Paula Gearon"}
    asami.internal
  (:require [asami.graph :as graph])
  #?(:clj (:import [java.util Date]
                   [java.util.concurrent.locks Lock])))

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
  [ms]
  #?(:clj (Date. ms) :cljs (js/Date. ms)))

(defn long-time
  "Converts a timestamp to a long value as the number of milliseconds"
  [t]
  (.getTime t))  ;; this is an identical operation in both Java and Javascript

(def project-args {:new-node graph/new-node
                   :node-label graph/node-label})

(defn project-ins-args
  [graph]
  (assoc project-args
         :resolve-pattern (partial graph/resolve-pattern graph)))

#?(:clj
   (defmacro with-lock
     "Uses a lock for a block of code"
     [lock & body]
     `(try
        (.lock ^Lock ~lock)
        ~@body
        (finally (.unlock ^Lock ~lock))))

   :cljs
   (defmacro with-lock
     [lock & body]
     `(do ~@body)))
