(ns ^{:doc "Encodes and decodes data for storage. Clojure implementation"
      :author "Paula Gearon"}
    asami.durable.encoder
  (:require [clojure.string :as s])
  (:import [clojure.lang Keyword BigInt]
           [java.io RandomAccessFile]
           [java.math BigInteger BigDecimal]
           [java.net URI]
           [java.time Instant]
           [java.util Date UUID]
           [java.nio ByteBuffer]
           [java.nio.charset Charset]))

(def utf8 (Charset/forName "UTF-8"))

(def type->code
  {Long (byte 0)
   Double (byte 1)
   String (byte 2)
   URI (byte 3)  ;; 4 & 5 are reserved for http and https URLs
   BigInt (byte 6)
   BigInteger (byte 6)
   BigDecimal (byte 7)
   Date (byte 8)
   Instant (byte 9)
   Keyword (byte 10)
   UUID (byte 11)
   :blob (byte 12)
   :xsd (byte 13)
   :pojo (byte 14)})

(def
  registered-xsd-types
  "A map of types to encoding functions"
  (atom {}))

(defn str-constructor?
  "Tests if a class has a constructor that takes a string"
  [c]
  (try
    (boolean (.getConstructor c (into-array Class [String])))
    (catch Throwable _ false)))

(defn type-code
  "Returns a code number for an object"
  [o]
  (if (bytes? o)
    [(type->code :blob) identity]
    (if-let [encoder (get @registered-xsd-types (type o))]
      [(type->code :xsd) encoder]
      (if (str-constructor? (type o))
        [(type->code :pojo) (fn [obj] (.getBytes (str (.getName (type o)) " " obj) utf8))]
        (throw (ex-info (str "Don't know how to encode a: " (type o)) {:object o}))))))

(defn general-header
  "Takes a type number and a length, and encodes to the general header style.
   Lengths 0-255   [2r1110tttt length]
   Lengths 256-32k [2r1111tttt (low-byte length) (high-byte length)]
   Lengths 32k-2G  [2r1111tttt (byte0 length) (byte1 length) (byte2 length) (byte3 length)]"
  [t len]
  (cond
    (<= len 0xFF)
    (byte-array [(bit-or 0xE0 t) len])
    (<= len 0x7FFF)
    (byte-array [(bit-or 0xF0 t) (bit-shift-right len 8) (bit-and 0xFF len)])
    :default
    (byte-array [(bit-or 0xF0 t)
                 (bit-and 0xFF (bit-shift-right len 24))
                 (bit-and 0xFF (bit-shift-right len 16))
                 (bit-and 0xFF (bit-shift-right len 8))
                 (bit-and 0xFF len)])))

(defprotocol FlatFile
  (header [this len] "Returns a byte array containing a header")
  (body [this] "Returns a byte array containing the encoded data"))

(extend-protocol FlatFile
  String
  (header [this len]
    (if (< len 0x80)
      (byte-array [len])
      (general-header (type->code String) len)))
  (body [^String this]
    (.getBytes this utf8))

  URI
  (header [this len]
    (if (< len 0x40)
      (byte-array [(bit-or 0x80 len)])
      (general-header (type->code URI) len)))
  (body [this]
    (.getBytes (str this) utf8))
  
  Keyword
  (header [this len]
    (if (< len 0x20)
      (byte-array [(bit-or 0xC0 len)])
      (general-header (type->code Keyword) len)))
  (body [this]
    (let [nms (namespace this)
          n (name this)]
      (.getBytes (if nms (str nms "/" n) n) utf8)))
  
  Long
  (header [this len]
    (assert (= len Long/BYTES))
    (byte-array [(bit-or 0xE0 (type->code Long))]))
  (body [^long this]
    (let [b (byte-array Long/BYTES)
          bb (ByteBuffer/wrap b)]
      (.putLong bb 0 this)
      b))

  Double
  (header [this len]
    (assert (= len Double/BYTES))
    (byte-array [(bit-or 0xE0 (type->code Double))]))
  (body [^double this]
    (let [b (byte-array Double/BYTES)
          bb (ByteBuffer/wrap b)]
      (.putDouble bb 0 this)
      b))

  BigInt
  (header [this len]
    (general-header (type->code BigInt) len))
  (body [this]
    (.toByteArray (biginteger this)))
  
  BigDecimal
  (header [this len]
    (general-header (type->code BigDecimal) len))
  (body [^BigDecimal this]
    (.getBytes (str this) utf8))

  Date
  (header [this len]
    (assert (= len Long/BYTES))
    (byte-array [(bit-or 0xE0 (type->code Date))]))
  (body [^Date this]
    (body (.getTime this)))

  Instant
  (header [this len]
    (assert (= len (+ Long/BYTES Integer/BYTES)))
    (byte-array [(bit-or 0xE0 (type->code Instant))]))
  (body [^Instant this]
    (let [b (byte-array (+ Long/BYTES Integer/BYTES))]
      (doto (ByteBuffer/wrap b)
        (.putLong 0 (.getEpochSecond this))
        (.putInt Long/BYTES (.getNano this)))
      b))

  UUID
  (header [this len]
    (assert (= len (* 2 Long/BYTES)))
    (byte-array [(bit-or 0xE0 (type->code UUID))]))
  (body [^UUID this]
    (let [b (byte-array (* 2 Long/BYTES))]
      (doto (ByteBuffer/wrap b)
        (.putLong 0 (.getLeastSignificantBits this))
        (.putLong Long/BYTES (.getMostSignificantBits this)))
      b))
  
  Object
  (header [this len]
    (let [tc (or (type->code (type this))
                 (first (type-code this)))]
      (general-header tc len)))
  (body [this]
    (if-let [tc (type->code (type this))]
      (.getBytes (str this) utf8)
      (if-let [[_ encoder] (type-code this)]
        (encoder this)))))

(defn to-bytes
  "Returns a tuple of byte arrays, representing the header and the body"
  [o]
  (let [b (body o)]
    [(header o (count b)) b]))
