(ns ^{:doc "Encodes and decodes data for storage. ClojureScript implementation"
      :author "Paula Gearon and Joel Holdbrooks"}
    asami.durable.encoder
  (:require [asami.bytes :as bytes]
            [clojure.string :as s])
  (:import [goog.math Long Integer]
           [goog Uri]))

(def ^{:private true}
  LONG_BYTES 8)

(def type->code
  {Long (byte 0)
   ;; Double (byte 1)
   js/String (byte 2)
   Uri (byte 3)  ;; 4 & 5 are reserved for http and https URLs
   ;; BigInt (byte 6)
   ;; BigInteger (byte 6)
   ;; BigDecimal (byte 7)
   js/Date (byte 8)
   ;; Instant (byte 9)
   Keyword (byte 10)
   UUID (byte 11)
   :blob (byte 12)
   ;; :xsd (byte 13)
   :pojo (byte 14)})

(def registered-xsd-types
  "A map of types to encoding functions"
  (atom {}))

(defprotocol FlatFile
  (header [this len] "Returns a byte array containing a header")
  (body [this] "Returns a byte array containing the encoded data"))

(defn pojo-encoder
  [o]
  (let [json-bytes (bytes/from-string (.stringify js/JSON o))
        length (bytes/byte-length json-bytes)
        b (js/ArrayBuffer. (+ bytes/NUMBER_BYTES length))
        v (js/DataView. b)]
    ;; Encode the length of the JSON into the output byte-array.
    (.setFloat64 v 0 length)
    ;; Copy the JSON bytes into the output byte-array.
    (run! (fn [[i byte]] (.setInt8 v (+ 8 i) byte))
          (map-indexed vector (js/Int8Array. json-bytes)))

    b))

(defn type-code
  "Returns a code number for an object"
  [o]
  (if (bytes/bytes? o)
    [(type->code :blob) identity]
    (if-let [encoder (get @registered-xsd-types (type o))]
      [(type->code :xsd) encoder]
      (if (object? o)
        [(type->code :pojo) pojo-encoder]
        (throw (ex-info (str "Don't know how to encode a: " (type o)) {:object o}))))))

(defn general-header
  "Takes a type number and a length, and encodes to the general header style.
   Lengths 0-255   [2r1110tttt length]
   Lengths 256-32k [2r1111tttt (low-byte length) (high-byte length)]
   Lengths 32k-2G  [2r1111tttt (byte0 length) (byte1 length) (byte2 length) (byte3 length)]"
  [t len]
  (cond
    (<= len 0xFF)
    (bytes/byte-array [(bit-or 0xE0 t) len])
    (<= len 0x7FFF)
    (bytes/byte-array [(bit-or 0xF0 t) (bit-shift-right len 8) (bit-and 0xFF len)])
    :default
    (bytes/byte-array [(bit-or 0xF0 t)
                       (bit-and 0xFF (bit-shift-right len 24))
                       (bit-and 0xFF (bit-shift-right len 16))
                       (bit-and 0xFF (bit-shift-right len 8))
                       (bit-and 0xFF len)])))

(extend-protocol FlatFile
  string
  (header [this len]
    (if (< len 0x80)
      (bytes/byte-array [len])
      (general-header (type->code js/String) len)))
  (body [this]
    (bytes/from-string this))

  Uri
  (header [this len]
    (if (< len 0x40)
      (bytes/byte-array [(bit-or 0x80 len)])
      (general-header (type->code Uri) len)))
  (body [this]
    (bytes/from-string (.toString this)))

  Keyword
  (header [this len]
    (if (< len 0x20)
      (bytes/byte-array [(bit-or 0xC0 len)])
      (general-header (type->code Keyword) len)))
  (body [this]
    (let [nms (namespace this)
          n (name this)]
      (bytes/from-string (if nms (str nms "/" n) n))))

  Long
  (header [this len]
    (assert (= len LONG_BYTES))
    (bytes/byte-array [(bit-or 0xE0 (type->code Long))]))
  (body [^long this]
    (let [b (bytes/byte-array LONG_BYTES)]
      (doto (js/DataView. b)
        (.setInt32 0 (.getHighBits this))
        (.setInt32 4 (.getLowBits this)))
      b))

  js/Date
  (header [this len]
    (assert (= len LONG_BYTES))
    (bytes/byte-array [(bit-or 0xE0 (type->code js/Date))]))
  (body [this]
    (body (.getTime this)))
  
  UUID
  (header [this len]
    (bytes/byte-array [(bit-or 0xE0 (type->code UUID))]))
  (body [^UUID this]
    (let [[a b c d e] (.split (str this) "-")
          least-significant-bits (.fromString Long (str a b c) 16)
          most-significant-bits (.fromString Long (str d e) 16)
          b (bytes/byte-array 16)]
      (doto (js/DataView. b)
        (.setInt32 0 (.getHighBits least-significant-bits))
        (.setInt32 4 (.getLowBits least-significant-bits))
        (.setInt32 8 (.getHighBits most-significant-bits))
        (.setInt32 12 (.getLowBits most-significant-bits)))
      b))

  number
  (header [this len])
  (body [this])

  object
  (header [this len]
    (let [tc (or (type->code (type this))
                 (first (type-code this)))]
      (general-header tc len)))
  (body [this]
    (if-let [tc (type->code (type this))]
      (bytes/from-string (str this))
      (if-let [[_ encoder] (type-code this)]
        (encoder this)))))

(defn to-bytes
  "Returns a tuple of byte arrays, representing the header and the body"
  [o]
  (let [b (body o)]
    [(header o (.-length b)) b]))
