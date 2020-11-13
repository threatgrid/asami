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
  (body [this] "Returns a byte array containing the encoded data")
  (encapsulated-id [this] "Returns an ID that encapsulates the type"))

;; Encapsualted IDs are IDs containing all of the information without requiring additional storage
;; The data type is contained in the top 4 bits. The remaining 60 bit hold the data:
;; Top 4 bits:
;; 1 0 0 0: long
;; 1 1 0 0: Date
;; 1 0 1 0: Instant
;; 1 1 1 0: Short String
;; 1 0 0 1: Short Keyword

(def ^:const long-type-mask 0x8000000000000000)
(def ^:const date-type-mask 0xC000000000000000)
(def ^:const inst-type-mask 0xA000000000000000)
(def ^:const sstr-type-mask 0xE000000000000000)
(def ^:const skey-type-mask 0x9000000000000000)
(def ^:const data-mask      0x0FFFFFFFFFFFFFFF)
(def ^:const max-short-long 0x07FFFFFFFFFFFFFF)
(def ^:const min-short-long 0xF800000000000000)

(def ^:const max-short-len 7)
(def ^:const sbytes-shift 48)

(def ^:const milli-nano "Number of nanoseconds in a millisecond" 1000000)

(defn encapsulate-sstr
  "Encapsulates a short string. If the string cannot be encapsulated, then return nil."
  [^String s]
  (when (<= (.length s) max-short-len)
    (let [abytes (.getBytes s utf8)
          len (count abytes)]
      (when (<= len max-short-len)
        (reduce (fn [v n] (bit-or v (bit-shift-left ^byte (aget abytes n) (- sbytes-shift n))))
                0 (range len))))))

(defn encapsulate-long
  "Encapsulates a long value. If the long is too large to be encapsulated, then return nil."
  [^long l]
  (when (and (< l max-short-long) (> l min-short-long))
    (bit-and data-mask l)))

(extend-protocol FlatFile
  String
  (header [this len]
    (if (< len 0x80)
      (byte-array [len])
      (general-header (type->code String) len)))
  (body [^String this]
    (.getBytes this utf8))
  (encapsulated-id [this]
    (when-let [sid (encapsulate-sstr this)]
      (bit-or sstr-type-mask sid)))

  URI
  (header [this len]
    (if (< len 0x40)
      (byte-array [(bit-or 0x80 len)])
      (general-header (type->code URI) len)))
  (body [this]
    (.getBytes (str this) utf8))
  (encapsulated-id [this] nil)
  
  Keyword
  (header [this len]
    (if (< len 0x20)
      (byte-array [(bit-or 0xC0 len)])
      (general-header (type->code Keyword) len)))
  (body [this]
    (let [nms (namespace this)
          n (name this)]
      (.getBytes (if nms (str nms "/" n) n) utf8)))
  (encapsulated-id [this]
    (when-let [sid (encapsulate-sstr (subs (str this) 1))]
      (bit-or skey-type-mask sid)))
  
  Long
  (header [this len]
    (assert (= len Long/BYTES))
    (byte-array [(bit-or 0xE0 (type->code Long))]))
  (body [^long this]
    (let [b (byte-array Long/BYTES)
          bb (ByteBuffer/wrap b)]
      (.putLong bb 0 this)
      b))
  (encapsulated-id [this]
    (when-let [v (encapsulate-long this)]
      (bit-or long-type-mask v)))

  Double
  (header [this len]
    (assert (= len Double/BYTES))
    (byte-array [(bit-or 0xE0 (type->code Double))]))
  (body [^double this]
    (let [b (byte-array Double/BYTES)
          bb (ByteBuffer/wrap b)]
      (.putDouble bb 0 this)
      b))
  (encapsulated-id [this] nil)

  BigInt
  (header [this len]
    (general-header (type->code BigInt) len))
  (body [this]
    (.toByteArray (biginteger this)))
  (encapsulated-id [this] nil)
  
  BigDecimal
  (header [this len]
    (general-header (type->code BigDecimal) len))
  (body [^BigDecimal this]
    (.getBytes (str this) utf8))
  (encapsulated-id [this] nil)

  Date
  (header [this len]
    (assert (= len Long/BYTES))
    (byte-array [(bit-or 0xE0 (type->code Date))]))
  (body [^Date this]
    (body (.getTime this)))
  (encapsulated-id [this]
    (when-let [v (encapsulate-long (.getTime ^Date this))]
      (bit-or date-type-mask v)))

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
  (encapsulated-id [this]
    (when-let [v (and (zero? (mod (.getNano ^Instant this) milli-nano))
                      (encapsulate-long (.toEpochMilli ^Instant this)))]
      (bit-or inst-type-mask v)))

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
  (encapsulated-id [this] nil)
  
  Object
  (header [this len]
    (let [tc (or (type->code (type this))
                 (first (type-code this)))]
      (general-header tc len)))
  (body [this]
    (if-let [tc (type->code (type this))]
      (.getBytes (str this) utf8)
      (if-let [[_ encoder] (type-code this)]
        (encoder this))))
  (encapsulated-id [this] nil))

(defn to-bytes
  "Returns a tuple of byte arrays, representing the header and the body"
  [o]
  (let [b (body o)]
    [(header o (count b)) b]))
