(ns ^{:doc "Encodes and decodes data for storage. Clojure implementation"
      :author "Paula Gearon"}
    asami.durable.decoder
  (:require [clojure.string :as s]
            [asami.durable.common :refer [read-byte read-bytes read-short]])
  (:import [clojure.lang Keyword BigInt]
           [java.math BigInteger BigDecimal]
           [java.net URI]
           [java.time Instant]
           [java.util Date UUID]
           [java.nio ByteBuffer]
           [java.nio.charset Charset]))


(def utf8 (Charset/forName "UTF-8"))

(defn decode-length
  "Reads the header to determine length.
  ext: if 0 then length is a byte, if 1 then length is in either a short or an int"
  ([ext paged-rdr ^long pos]
   (if ext
     (let [raw (read-byte paged-rdr pos)]
       [Byte/BYTES (bit-and 0xFF raw)])
     (let [len (read-short paged-rdr pos)]
       (if (< len 0)
         (let [len2 (read-short paged-rdr pos)]
           [Integer/BYTES (bit-or
                           (bit-shift-left (int (bit-and 0x7FFF len)) Short/SIZE)
                           len2)])
         [Short/BYTES len]))))
  ([^bytes data]
   (if (zero? (bit-and 0x10 (aget data 0)))
     (let [raw (aget data 1)]
       [Byte/BYTES (bit-and 0xFF raw)])
     (let [len (bit-or (bit-shift-left (aget data 1) 8)
                       (bit-and 0xFF (aget data 2)))]
       (if (< len 0)
         [Integer/BYTES (bit-or
                         (bit-shift-left (int (bit-and 0x7FFF len)) Short/SIZE)
                         (bit-shift-left (aget data 3) 8)
                         (bit-and 0xFF (aget data 4)))]
         [Short/BYTES len])))))

;; Readers are given the length and a position. They then read data into a type

(defn read-str
  [paged-rdr ^long pos ^long len]
  (String. (read-bytes paged-rdr pos len) utf8))

(defn read-uri
  [paged-rdr ^long pos ^long len]
  (URI/create (read-str paged-rdr pos len)))

(defn read-keyword
  [paged-rdr ^long pos ^long len]
  (keyword (read-str paged-rdr pos len)))

;; decoders operate on the bytes following the initial type byte information
;; if the data type has variable length, then this is decoded first

(defn long-decoder
  [ext paged-rdr ^long pos]
  (let [b (ByteBuffer/wrap (read-bytes paged-rdr pos Long/BYTES))]
    (.getLong b 0)))

(defn double-decoder
  [ext paged-rdr ^long pos]
  (let [b (ByteBuffer/wrap (read-bytes paged-rdr pos Long/BYTES))]
    (.getDouble b 0)))

(defn string-decoder
  [ext paged-rdr ^long pos]
  (let [[i len] (decode-length ext paged-rdr pos)]
    (read-str paged-rdr (+ pos i) len)))

(defn uri-decoder
  [ext paged-rdr ^long pos]
  (let [[i len] (decode-length ext paged-rdr pos)]
    (read-uri paged-rdr (+ pos i) len)))

(defn bigint-decoder
  [ext paged-rdr ^long pos]
  (let [[i len] (decode-length ext paged-rdr pos)
        b (read-bytes paged-rdr (+ i pos) len)]
    (bigint (BigInteger. b))))

(defn bigdec-decoder
  [ext paged-rdr ^long pos]
  (bigdec (string-decoder ext paged-rdr pos)))

(defn date-decoder
  [ext paged-rdr ^long pos]
  (Date. (long-decoder ext paged-rdr pos)))

(def ^:const instant-length (+ Long/BYTES Integer/BYTES))

(defn instant-decoder
  [ext paged-rdr ^long pos]
  (let [b (ByteBuffer/wrap (read-bytes paged-rdr pos instant-length))
        epoch (.getLong b 0)
        sec (.getInt b Long/BYTES)]
    (Instant/ofEpochSecond epoch sec)))

(defn keyword-decoder
  [ext paged-rdr ^long pos]
  (let [[i len] (decode-length ext paged-rdr pos)]
    (read-keyword paged-rdr (+ pos i) len)))

(def ^:const uuid-length (* 2 Long/BYTES))

(defn uuid-decoder
  [ext paged-rdr ^long pos]
  (let [b (ByteBuffer/wrap (read-bytes paged-rdr pos uuid-length))
        low (.getLong b 0)
        high (.getLong b Long/BYTES)]
    (UUID. high low)))

(defn blob-decoder
  [ext paged-rdr ^long pos]
  (let [[i len] (decode-length ext paged-rdr pos)]
    (read-bytes paged-rdr (+ i pos) len)))

(defn xsd-decoder
  [ext paged-rdr ^long pos]
  (let [s (string-decoder ext paged-rdr pos)
        sp (s/index-of s \space)]
    [(URI/create (subs s 0 sp)) (subs (inc sp))]))

(defn default-decoder
  "This is a decoder for unsupported data that has a string constructor"
  [ext paged-rdr ^long pos]
  (let [s (string-decoder ext paged-rdr pos)
        sp (s/index-of s \space)
        class-name (subs s 0 sp)]
    (try
      (let [c (Class/forName class-name) 
            cn (.getConstructor c (into-array Class [String]))]
        (.newInstance cn (object-array [(subs s (inc sp))])))
      (catch Exception e
        (throw (ex-info (str "Unable to construct class: " class-name) {:class class-name}))))))

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

(def ^:const type-nybble-shift 60)
(def ^:const len-nybble-shift 56)
(def ^:const sbytes-shift 48)

(def ^:const byte-mask 0xFF)
(def ^:const nybble-mask 0xF)
(def ^:const long-type 0x8)
(def ^:const date-type 0xC)
(def ^:const inst-type 0xA)
(def ^:const sstr-type 0xE)
(def ^:const skey-type 0x9)
(def ^:const data-mask 0x0FFFFFFFFFFFFFFF)
(def ^:const long-nbit 0x0800000000000000)
(def ^:const lneg-bits 0xF000000000000000)

(defn extract-long
  "Extract a long number from an encapsulating ID"
  [^long id]
  (let [l (bit-and data-mask id)]
    (if (zero? (bit-and long-nbit l))
      l
      (bit-or lneg-bits l))))

(defn as-byte
  [n]
  (if (zero? (bit-and 0x80 n))
    (byte n)
    (byte (bit-or -0x100 n))))

(defn extract-sstr
  "Extract a short string from an encapsulating ID"
  [^long id]
  (let [len (bit-and (bit-shift-right id len-nybble-shift) nybble-mask)
        abytes (byte-array len)]
    (doseq [i (range len)]
      (aset abytes i
            (->> (* i Byte/SIZE)
                 (- sbytes-shift)
                 (bit-shift-right id)
                 (bit-and byte-mask)
                 as-byte)))
    (String. abytes 0 len utf8)))

(defn unencapsulate-id
  "Converts an encapsulating ID into the object it encapsulates. Return nil if it does not encapsulate anything."
  [^long id]
  (when (> 0 id)
    (let [tb (bit-shift-right id type-nybble-shift)]
      (case tb
        long-type (extract-long id)
        date-type (Date. (extract-long id))
        inst-type (Instant/ofEpochMilli (extract-long id))
        sstr-type (extract-sstr id)
        skey-type (keyword (extract-sstr id))
        nil))))

(defn type-info
  "Returns the type information encoded in a header-byte"
  [b]
  (cond
    (zero? (bit-and 0x80 b)) 2   ;; string
    (zero? (bit-and 0x40 b)) 3   ;; uri
    (zero? (bit-and 0x20 b)) 10  ;; keyword
    :default (let [tn (bit-and 0xF b)]
               (if (or (= tn 4) (= tn 5)) 3 tn))))

(defn string-style-compare
  [left-s right-bytes]
  (let [rbc (count right-bytes)
        [rn rlen] (decode-length right-bytes)
        ;; TODO Check if the final byte is part of a unicode pair
        right-s (String. right-bytes rn (min (- rbc rn) rlen) utf8)
        min-len (min (count left-s) rlen (count right-s))]
    (compare (subs left-s 0 min-len)
             (subs right-s 0 min-len))))

(defn long-bytes-compare
  "Compare data from 2 values that are the same type. If the data cannot give a result
   then return 0."
  [type-left left-header left-body left-object right-bytes]
  (case type-left
    2 (string-style-compare left-object right-bytes)
    3 (string-style-compare (str left-object) right-bytes)
    10 (string-style-compare (subs (str left-object) 1) right-bytes)
    ))

(defn read-object
  "Reads an object from a paged-reader, at id=pos"
  [paged-rdr ^long pos]
  (let [b0 (read-byte paged-rdr pos)
        ipos (inc pos)]
    (cond
      (zero? (bit-and 0x80 b0)) (read-str paged-rdr ipos b0)
      (zero? (bit-and 0x40 b0)) (read-uri paged-rdr ipos (bit-and 0x3F b0))
      (zero? (bit-and 0x20 b0)) (read-keyword paged-rdr ipos (bit-and 0x1F b0))
      :default ((typecode->decoder (bit-and 0x0F b0) default-decoder)
                (zero? (bit-and 0x10 b0)) paged-rdr ipos))))
