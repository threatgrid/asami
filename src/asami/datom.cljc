(ns ^{:doc "Encapsulates the implementation of the Datom type"
      :author "Paula Gearon"}
    asami.datom
    (:require [clojure.string :as str]
              #?(:clj  [schema.core :as s]
                 :cljs [schema.core :as s :include-macros true])
              #?(:cljs [cljs.reader :as reader]))
    #?(:clj (:import [clojure.lang Associative Indexed]
                     [java.io Writer])))

(defprotocol Vectorizable
  (as-vec [o] "Converts object to a vector"))

(declare ->Datom)

;; simple type to represent the assertion or removal of data
#?(:clj
   (deftype Datom [e a v tx added]
     Vectorizable
     (as-vec [_] [e a v tx added])

     Associative
     (containsKey [_ i] (and (int? i) (<= 0 i) (< i 5)))
     (entryAt [_ i] (nth (as-vec i)))
     (assoc [this k v] (->Datom (assoc as-vec this k v)))
     (count [_] 5)
     (cons [this c] (cons c (as-vec this)))
     (empty [_] (throw (ex-info "Unsupported Operation" {})))
     (equiv [this o] (= (as-vec this) (as-vec o)))
     (valAt [this n] (nth (as-vec this) n))
     (valAt [this n not-found] (if (and (int? n) (<= 0 n) (< n 5)) (nth (as-vec this) n) not-found))
     Indexed
     (nth [this n] (nth (as-vec this) n))
     (nth [this n not-found] (if (and (int? n) (<= 0 n) (< n 5)) (nth (as-vec this) n) not-found))
     
     Object
     (toString [this]
       (let [data (prn-str [e a v tx (if added :db/add :db/retract)])]
         (str "#datom " data))))

   (deftype Datom [entity attribute value tx-id action]
     Vectorizable
     (as-vec [_] [entity attribute value tx-id action])

     IAssociative
     (-contains-key? [_ i] (and (integer? i) (<= 0 i) (< i 5)))
     (-assoc [this k v] (Datom. (assoc as-vec this k v)))
     
     Object
     (toString [this]
       (let [data (prn-str [entity attribute value tx-id (if action :db/add :db/retract)])]
         (str "#datom " data)))))

(defn new-datom [e a v tx-id action])

(defn datom-reader
  [[e a v tx action]]
  (->Datom e a v tx (= action :db/add)))

(defmethod clojure.core/print-method asami.datom.Datom [o ^Writer w]
  ((deref #'clojure.core/pr-on) "#datom " w)
  (print-method (as-vec o) w))

(defmethod clojure.core/print-dup asami.datom.Datom [o ^Writer w]
  ((deref #'clojure.core/pr-on) "#asami/datom " w)
  (print-method (as-vec o) w))
