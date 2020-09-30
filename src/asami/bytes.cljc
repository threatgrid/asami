(ns asami.bytes
  "Utilities for working with bytes."
  (:refer-clojure :exclude [byte-array bytes?])
  (:require [clojure.core :as clojure])
  #?(:clj (:import [java.nio ByteBuffer])
     :cljs (:import [goog.math Long])))

(def BYTE_BYTES 1)
(def SHORT_BYTES 1)
(def SHORT_SIZE 16)
(def INTEGER_BYTES 4)
(def LONG_BYTES 8)
(def NUMBER_BYTES 8)

(defn byte-array
  "Cross-platform implementation of `clojure.core/byte-array`. The
  Clojure implementation passes arguments to
  `clojure.core/byte-array`. For ClojureScript returns an instance of
  `js/ArrayBuffer`."
  ([size-or-seq]
   #?(:clj (clojure/byte-array size-or-seq)
      :cljs
      (if (number? size-or-seq)
        (js/ArrayBuffer. size-or-seq)
        (.-buffer (.from js/Int8Array (to-array size-or-seq))))))
  ([size init-val-or-seq]
   #?(:clj (clojure/byte-array size init-val-or-seq)
      :cljs
      (let [b (js/ArrayBuffer. size)
            a (to-array (take size init-val-or-seq))
            v (js/DataView. b)]
        (.forEach a (fn [x i] (.setInt8 v i x)))
        b))))

(defn byte-length
  "Return the number of bytes in the byte array `b`."
  [b]
  #?(:clj (count b)
     :cljs (or (.-byteLength b)
               (count b))))

(defn bytes?
  "true if `x` is a byte array, false otherwise."
  [x]
  #?(:clj (clojure/bytes? x)
     :cljs (instance? js/ArrayBuffer x)))

#?(:cljs
   (deftype ByteBuffer [^js/DataView __dataView __position __limit __readOnly]
     Object
     (asReadOnlyBuffer [this]
       (ByteBuffer. __dataView __position __limit true))

     (get [this]
       (if (not (< __position __limit))
         (throw (ex-info "Buffer underflow" {:currentLimit __limit
                                             :currentPosition __position})))

       (let [byte (.getInt8 __dataView __position)]
         (set! (.-__position this) (inc __position))

         byte))

     (get [this number-or-destination]
       (if (number? number-or-destination)
         (.getInt8 __dataView number-or-destination)
         (.get this number-or-destination 0 (byte-length number-or-destination))))

     (get [this destination offset length]
       (dotimes [i length]
         (aset destination (+ offset i) (.get this)))

       this)

     (getShort [this]
       (let [short (.getInt16 __dataView __position)]
         (set! (.-__position this) (+ 2 __position))
         short))

     (getShort [this offset]
       (let [short (.getInt16 __dataView offset)]
         short))

     (position [this]
       __position)

     (position [this newPosition]
       (if (neg? newPosition)
         (throw (ex-info "New position must be non-negative" {:newPosition newPosition})))
       
       (if (< __limit newPosition)
         (throw (ex-info "New position must be less that the current limit"
                         {:currentLimit __limit
                          :newPosition newPosition})))

       (set! (.-__position this) newPosition)

       this)

     (put [this byte-or-bytes]
       (if (seqable? byte-or-bytes)
         (.put this byte-or-bytes 0 (byte-length byte-or-bytes))
         (do (.setInt8 __dataView __position byte-or-bytes)
             (set! (.-__position this) (inc __position))
             this)))

     (put [this bytes offset length]
       (doseq [byte (take length (drop offset bytes))]
         (.setInt8 __dataView __position byte)
         (set! (.-__position this) (inc __position)))

       this)

     (putShort [this short]
       (.setInt16 __dataView __position short)
       (set! (.-__position this) (+ 2 __position))
       this)

     (putShort [this index short]
       (.setInt16 __dataView index short)
       this)))

(defn byte-buffer
  "Cross-platform constructor which returns an object which behaves
  like a `java.nio.ByteBuffer`. The Clojure constructor passes the
  argument to `ByteBuffer/wrap`. The ClojureScript constructor creates
  an instance of the `ByteBuffer` type implemented in this namespace."
  [byte-array]
  #?(:clj (ByteBuffer/wrap byte-array)
     :cljs (->ByteBuffer (js/DataView. byte-array) 0 (.-byteLength byte-array) false)))

(defn from-long
  [l]
  #?(:cljs
     (let [b (js/ArrayBuffer. 8)]
       (doto (js/DataView. b)
         (.setInt32 0 (.getHighBits l))
         (.setInt32 4 (.getLowBits l)))
       b)))

(defn to-long
  [byte-array]
  #?(:cljs
     (let [v (js/DataView. byte-array)
           high-bits (.getInt32 v 0)
           low-bits (.getInt32 v 4)]
       (.fromBits Long low-bits high-bits))))

(defn from-string
  [s]
  #?(:cljs
     (let [encoder (js/TextEncoder.)]
       (.-buffer (.encode encoder s)))))

(defn to-string
  [byte-array]
  #?(:cljs
     (let [decoder (js/TextDecoder.)]
       (.decode decoder byte-array))))

(defn from-number
  [n]
  #?(:cljs
     (let [b (js/ArrayBuffer. NUMBER_BYTES)]
       (.setFloat64 (js/DataView. b) 0 n)
       b)))

(defn to-number
  [byte-array]
  #?(:cljs
     (.getFloat64 (js/DataView. byte-array) 0)))
