(ns ^{:doc "Encodes and decodes data for storage. ClojureScript implementation"
      :author "Paula Gearon and Joel Holdbrooks"}
    asami.durable.encoder
  (:require [clojure.string :as s])
  (:import [goog.math Long Integer]
           [goog Uri]))

;; (set! *warn-on-reflection* true)

(def ^{:private true} LONG_BYTES 8)

(defn byte-array [size-or-seq]
  (if (number? size-or-seq)
    (js/Int8Array. (js/ArrayBuffer. size-or-seq))
    (.from js/Int8Array size-or-seq)))

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
   ;; :blob (byte 12)
   ;; :xsd (byte 13)
   ;; :pojo (byte 14)
   })

(defprotocol FlatFile
  (header [this len] "Returns a byte array containing a header")
  (body [this] "Returns a byte array containing the encoded data"))

(defn type-code
  "Returns a code number for an object"
  [o]
  (throw (ex-info "Not implemented" {}))
  #_
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

(defn int->bytes
  {:private true}
  [i]
  [(bit-and (bit-shift-right i 24) 0xFF)
   (bit-and (bit-shift-right i 16) 0xFF)
   (bit-and (bit-shift-right i 8) 0xFF)
   (bit-and (bit-shift-right i 0) 0xFF)])

(defn long->bytes
  {:private true}
  [l]
  (byte-array (concat (int->bytes (.getHighBits l))
                      (int->bytes (.getLowBits l)))))

(defn str->bytes
  {:private true}
  [s]
  (let [encoder (js/TextEncoder.)]
    (.encode encoder s)))

(extend-protocol FlatFile
  string
  (header [this len]
    (if (< len 0x80)
      (byte-array [len])
      (general-header (type->code js/String) len)))
  (body [this]
    (str->bytes this))

  Uri
  (header [this len]
    (if (< len 0x40)
      (byte-array [(bit-or 0x80 len)])
      (general-header (type->code Uri) len)))
  (body [this]
    (str->bytes (.toString this)))

  Keyword
  (header [this len]
    (if (< len 0x20)
      (byte-array [(bit-or 0xC0 len)])
      (general-header (type->code Keyword) len)))
  (body [this]
    (let [nms (namespace this)
          n (name this)]
      (str->bytes (if nms (str nms "/" n) n))))

  js/Date
  (header [this len]
    (assert (= len LONG_BYTES))
    (byte-array [(bit-or 0xE0 (type->code js/Date))]))
  (body [this]
    (body (.getTime this)))
  
  UUID
  (header [this len]
    (byte-array [(bit-or 0xE0 (type->code UUID))]))
  (body [^UUID this]
    (let [[a b c d e] (.split (str this) "-")]
      (let [least-significant-bits (.fromString Long (str a b c) 16)
            most-significant-bits (.fromString Long (str d e) 16)]
        (byte-array
         (concat (int->bytes (.getHighBits least-significant-bits))
                 (int->bytes (.getLowBits least-significant-bits))
                 (int->bytes (.getHighBits most-significant-bits))
                 (int->bytes (.getLowBits most-significant-bits)))))))

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
      (str->bytes (str this))
      (if-let [[_ encoder] (type-code this)]
        (encoder this)))))

(defn to-bytes
  "Returns a tuple of byte arrays, representing the header and the body"
  [o]
  (let [b (body o)]
    [(header o (.-length b)) b]))

(defn encapsulate-id [x])
