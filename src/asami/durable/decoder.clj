(ns ^{:doc "Encodes and decodes data for storage. Clojure implementation"
      :author "Paula Gearon"}
    asami.durable.decoder
  (:require [clojure.string :as s])
  (:import [clojure.lang Keyword BigInt]
           [java.io RandomAccessFile]
           [java.math BigInteger BigDecimal]
           [java.net URI]
           [java.time Instant]
           [java.util Date UUID]
           [java.nio ByteBuffer]
           [java.nio.charset Charset]))


;; TODO: change to using Paged files

(def utf8 (Charset/forName ("UTF-8")))

(defn decode-length
  [^bool ext ^RandomAccessFile f]
  (if ext
    (let [len (.readShort f)]
      (if (< len 0)
        (let [len2 (.readUnsignedShort f)]
          (bit-or
           (bit-shift-left (int (bit-and 0x7FFF len)) len 16)
           len2))
        len))
    (int (.readByte f))))

(defn read-string
  [^RandomAccessFile f ^long len]
  (let [b (byte-array len)]
    (.readFully f b)
    b))

(defn read-uri
  [^RandomAccessFile f ^long len]
  (URI/create (read-string len)))

(defn read-keyword
  [^RandomAccessFile f ^long len]
  (keyword (read-string len)))

(defn long-decoder
  [^bool ext ^RandomAccessFile f]
  (.readLong f))

(defn double-decoder
  [^bool ext ^RandomAccessFile f]
  (.readDouble f))

(defn string-decoder
  [^bool ext ^RandomAccessFile f]
  (read-string f (decode-length ext f)))

(defn uri-decoder
  [^bool ext ^RandomAccessFile f]
  (read-uri f (decode-length ext f)))

(defn bigint-decoder
  [^bool ext ^RandomAccessFile f]
  (let [len (decode-length ext f)
        b (byte-array len)]
    (.readFully f b)
    (bigint (BigInteger. b))))

(defn bigdec-decoder
  [^bool ext ^RandomAccessFile f]
  (big-decimal (string-decoder ext f)))

(defn date-decoder
  [^bool ext ^RandomAccessFile f]
  (Date. (.readLong f)))

(defn instant-decoder
  [^bool ext ^RandomAccessFile f]
  (let [epoch (.readLong f)
        sec (.readInt f)]
    (Instant/ofEpochSecond epoch sec)))

(defn keyword-decoder
  [^bool ext ^RandomAccessFile f]
  (read-keyword f (decode-length ext f)))

(defn uuid-decoder
  [^bool ext ^RandomAccessFile f]
  (let [low (.readLong f)
        high (.readLong f)]
    (UUID. high low)))

(defn blob-decoder
  [^bool ext ^RandomAccessFile f]
  (let [b (byte-array (decode-length ext f))]
    (.readFully f b)
    b))

(defn xsd-decoder
  [^bool ext ^RandomAccessFile f]
  (let [s (string-decoder ext f)
        sp (s/index-of s \space)]
    [(URI/create (subs s 0 sp)) (inc sp)]))

(def typecode->decoder
  "Map of type codes to decoder functions"
  {0 long-decoder
   1 double-decoder
   2 string-decoder
   3 uri-decoder
   6 bigint-decoder
   7 bigdec-decoder
   8 date-decoder
   9 instant-decoder
   10 keyword-decoder
   11 uuid-decoder
   12 blob-decoder
   13 xsd-decoder})

(defn read-object
  [^RandomAccessFile f ^long pos]
  (.seek f pos)
  (let [b0 (.readByte f)]
    (cond
      (zero? (bit-and 0x80 b0)) (read-string f b0)
      (zero? (bit-and 0x40 b0)) (read-uri f (bit-and 0x3F b0))
      (zero? (bit-and 0x20 b0)) (read-keyword f (bit-and 0x1F b0))
      :default ((typecode->decoder (bit-and 0x0F b0) default-decoder)
                (zero? (bit-and 0x10 b0)) f))))
