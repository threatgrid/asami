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
     (containsKey [_ i] (if (int? i)
                          (and (<= 0 i) (< i 5))
                          (#{:e :a :v :tx :added} i)))
     (entryAt [_ i] (nth (as-vec i)))
     (assoc [this k v] (apply ->Datom (assoc (as-vec this) ({:e 0 :a 1 :v 2 :tx 3 :added 4} k k) v)))
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
         (str "#datom " data)))))

#?(:cljs
   (deftype Datom [e a v tx added]
     Vectorizable
     (as-vec [_] [e a v tx added])

     IAssociative
     (-contains-key? [_ i] (if (integer? i)
                             (and (<= 0 i) (< i 5))
                             (#{:e :a :v :tx :added} i)))
     (-assoc [this k v]
       (apply ->Datom (assoc (as-vec this) ({:e 0 :a 1 :v 2 :tx 3 :added 4} k k) v)))

     Object
     (toString [this]
       (let [data (prn-str [e a v tx (if added :db/add :db/retract)])]
         (str "#datom " data)))
     (equiv [this other]
       (-equiv this other))
     (indexOf [coll x]
       (-indexOf (as-vec coll) x 0))
     (indexOf [coll x start]
       (-indexOf (as-vec coll) x start))
     (lastIndexOf [coll x]
       (-lastIndexOf (as-vec coll) x 5))
     (lastIndexOf [coll x start]
       (-lastIndexOf (as-vec coll) x start))

     ISeqable
     (-seq [coll] (seq (as-vec coll)))

     ICounted
     (-count [coll] 5)

     IIndexed
     (-nth [coll n]
       (nth (as-vec coll) n))
     (-nth [coll n not-found]
       (nth (as-vec coll) n not-found))

     ILookup
     (-lookup [coll k] (-lookup coll k nil))
     (-lookup [coll k not-found] (-lookup (as-vec coll) k not-found))

     APersistentVector
     IVector
     (-assoc-n [coll n val]
       (-assoc-n (as-vec coll) n val))

     ICollection
     (-conj [coll o] 
       (conj (as-vec coll) o))))


(defn datom-reader
  [[e a v tx added]]
  (->Datom e a v tx (= added :db/add)))

#?(:clj
   (defmethod clojure.core/print-method asami.datom.Datom [o ^Writer w]
     (.write w "#datom ")
     (print-method (as-vec o) w)))

#?(:clj
   (defmethod clojure.core/print-dup asami.datom.Datom [o ^Writer w]
     (.write w "#asami/datom ")
     (print-method (as-vec o) w)))
