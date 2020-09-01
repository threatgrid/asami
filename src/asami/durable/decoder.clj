(ns ^{:doc "Encodes and decodes data for storage. Clojure implementation"
      :author "Paula Gearon"}
    asami.durable.decoder
  (:require [clojure.string :as s]
            [asami.durable.pages :refer [read-byte read-bytes read-short]])
  (:import [clojure.lang Keyword BigInt]
           [java.math BigInteger BigDecimal]
           [java.net URI]
           [java.time Instant]
           [java.util Date UUID]
           [java.nio ByteBuffer]
           [java.nio.charset Charset]))


(def utf8 (Charset/forName ("UTF-8")))

(defn decode-length
  [^bool ext paged-rdr ^long pos]
  (if ext
    (let [len (read-short paged-rdr pos)]
      (if (< len 0)
        (let [len2 (read-short paged-rdr pos)]
          [Integer/BYTES (bit-or
                          (bit-shift-left (int (bit-and 0x7FFF len)) len Short/SIZE)
                          len2)])
        [Short/BYTES len]))
    [Byte/BYTES (int (read-byte paged-rdr pos))]))

;; Readers are given the length and a position. They then read data into a type

(defn read-string
  [paged-rdr ^long pos ^long len]
  (String. (read-bytes f pos len) utf8))

(defn read-uri
  [paged-rdr ^long pos ^long len]
  (URI/create (read-string paged-rdr pos len)))

(defn read-keyword
  [paged-rdr ^long pos ^long len]
  (keyword (read-string paged-rdr pos len)))

;; decoders operate on the bytes following the initial type byte information
;; if the data type has variable length, then this is decoded first

(defn long-decoder
  [^bool ext paged-rdr ^long pos]
  (let [b (ByteBuffer/wrap (read-bytes paged-rdr pos Long/BYTES))]
    (.getLong b 0)))

(defn double-decoder
  [^bool ext paged-rdr ^long pos]
  (let [b (ByteBuffer/wrap (read-bytes paged-rdr pos Long/BYTES))]
    (.getDouble b 0)))

(defn string-decoder
  [^bool ext paged-rdr ^long pos]
  (let [[i len] (decode-length ext paged-rdr pos)]
    (read-string paged-rdr (+ pos i) len)))

(defn uri-decoder
  [^bool ext paged-rdr ^long pos]
  (let [[i len] (decode-length ext paged-rdr pos)]
    (read-uri paged-rdr (+ pos i) len)))

(defn bigint-decoder
  [^bool ext paged-rdr ^long pos]
  (let [[i len] (decode-length ext paged-rdr pos)
        b (read-bytes paged-rdr (+ i pos) len)]
    (bigint (BigInteger. b))))

(defn bigdec-decoder
  [^bool ext paged-rdr ^long pos]
  (big-decimal (string-decoder ext paged-rdr pos)))

(defn date-decoder
  [^bool ext paged-rdr ^long pos]
  (Date. (long-decoder ext paged-rdr pos)))

(defn instant-decoder
  [^bool ext paged-rdr ^long pos]
  (let [b (ByteBuffer/wrap (read-bytes paged-rdr pos #=(+ Long/BYTES Integer/BYTES)))
        epoch (.getLong b 0)
        sec (.getInt b Long/BYTES)]
    (Instant/ofEpochSecond epoch sec)))

(defn keyword-decoder
  [^bool ext paged-rdr ^long pos]
  (let [[i len] (decode-length ext paged-rdr pos)]
    (read-keyword paged-rdr (+ pos i) len)))

(defn uuid-decoder
  [^bool ext paged-rdr ^long pos]
  (let [b (ByteBuffer/wrap (read-bytes paged-rdr pos #=(* 2 Long/BYTES)))
        low (.getLong b 0)
        high (.getLong b Long/BYTES)]
    (UUID. high low)))

(defn blob-decoder
  [^bool ext paged-rdr ^long pos]
  (let [[i len] (decode-length ext paged-rdr pos)]
    (read-bytes paged-rdr (+ i pos) len)))

(defn xsd-decoder
  [^bool ext paged-rdr ^long pos]
  (let [s (string-decoder ext paged-rdr pos)
        sp (s/index-of s \space)]
    [(URI/create (subs s 0 sp)) (subs (inc sp))]))

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
  "Reads an object from a paged-reader, at id=pos"
  [paged-rdr ^long pos]
  (let [b0 (read-byte paged-rdr pos)
        ipos (inc pos)]
    (cond
      (zero? (bit-and 0x80 b0)) (read-string paged-rdr ipos b0)
      (zero? (bit-and 0x40 b0)) (read-uri paged-rdr ipos (bit-and 0x3F b0))
      (zero? (bit-and 0x20 b0)) (read-keyword paged-rdr ipos (bit-and 0x1F b0))
      :default ((typecode->decoder (bit-and 0x0F b0) default-decoder)
                (zero? (bit-and 0x10 b0)) paged-rdr ipos))))
