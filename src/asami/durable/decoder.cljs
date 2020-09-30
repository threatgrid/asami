(ns ^{:doc "Encodes and decodes data for storage. Clojure implementation"
      :author "Paula Gearon and Joel Holdbrooks"}
    asami.durable.decoder
  (:require [asami.bytes :as bytes]
            [asami.durable.pages :refer [read-byte read-bytes read-short]]
            [clojure.string :as s])
  (:import [goog.math Long Integer]
           [goog Uri]))

(defn decode-length
  "Reads the header to determine length.
  ext: if 0 then length is a byte, if 1 then length is in either a short or an int"
  [ext paged-rdr ^long pos]
  (if ext
    (let [raw (read-byte paged-rdr pos)]
      [bytes/BYTE_BYTES (bit-and 0xFF raw)])
    (let [len (read-short paged-rdr pos)]
      (if (< len 0)
        (let [len2 (read-short paged-rdr pos)]
          [bytes/INTEGER_BYTES (bit-or (bit-shift-left (int (bit-and 0x7FFF len)) bytes/SHORT_SIZE)
                                       len2)])
        [bytes/SHORT_BYTES len]))))

;; Readers are given the length and a position. They then read data into a type

(defn read-str
  [paged-rdr pos len]
  (bytes/to-string (read-bytes paged-rdr pos len)))

(defn read-uri
  [paged-rdr pos len]
  (Uri/parse (read-str paged-rdr pos len)))

(defn read-keyword
  [paged-rdr pos len]
  (keyword (read-str paged-rdr pos len)))

;; decoders operate on the bytes following the initial type byte information
;; if the data type has variable length, then this is decoded first

(defn long-decoder
  [ext paged-rdr pos]
  (bytes/to-long (read-bytes paged-rdr pos bytes/LONG_BYTES)))

(defn number-decoder
  [ext paged-rdr pos]
  (bytes/to-number (read-bytes paged-rdr pos bytes/NUMBER_BYTES)))

(defn double-decoder
  [ext paged-rdr pos]
  (let [b (read-bytes paged-rdr pos bytes/LONG_BYTES)]
    (.getFloat64 (js/DataView. b) 0)))

(defn string-decoder
  [ext paged-rdr ^long pos]
  (let [[i len] (decode-length ext paged-rdr pos)]
    (read-str paged-rdr (+ pos i) len)))

(defn uri-decoder
  [ext paged-rdr ^long pos]
  (let [[i len] (decode-length ext paged-rdr pos)]
    (read-uri paged-rdr (+ pos i) len)))

#_
(defn bigint-decoder
  [ext paged-rdr ^long pos]
  (let [[i len] (decode-length ext paged-rdr pos)
        b (read-bytes paged-rdr (+ i pos) len)]
    (bigint (BigInteger. b))))

#_
(defn bigdec-decoder
  [ext paged-rdr ^long pos]
  (bigdec (string-decoder ext paged-rdr pos)))

(defn date-decoder
  [ext paged-rdr ^long pos]
  ;; Note: `.toNumber` may not be safe here.
  (js/Date. (.toNumber (long-decoder ext paged-rdr pos))))

(def ^:const instant-length (+ bytes/LONG_BYTES bytes/INTEGER_BYTES))

(defn instant-decoder
  [ext paged-rdr ^long pos]
  (let [b (read-bytes paged-rdr pos instant-length)
        ;; epoch (.getLong b 0) ;; Ignored for now.
        sec (bytes/to-long b)]
    (js/Date. (* 1000 sec))))

(defn keyword-decoder
  [ext paged-rdr ^long pos]
  (let [[i len] (decode-length ext paged-rdr pos)]
    (read-keyword paged-rdr (+ pos i) len)))

(def ^:const uuid-length
  (* 2 bytes/LONG_BYTES))

(defn uuid-decoder
  [ext paged-rdr ^long pos]
  (let [b (read-bytes paged-rdr pos uuid-length)
        hex (s/join (map (fn [b]
                           (let [hex (.toString b 16)]
                             (if (<= b 0xf)
                               (str 0 hex)
                               hex)))
                         b))]
    (if-let [[_ a b c d e] (re-matches #"(.{8})(.{4})(.{4})(.{4})(.{12})" hex)]
      (uuid (str a "-" b "-" c "-" d "-" e))
      ;; TODO: error handling
      )))

(defn blob-decoder
  [ext paged-rdr ^long pos]
  (let [[i len] (decode-length ext paged-rdr pos)]
    (read-bytes paged-rdr (+ i pos) len)))

(defn xsd-decoder
  [ext paged-rdr ^long pos]
  (let [s (string-decoder ext paged-rdr pos)
        sp (s/index-of s \space)]
    [(Uri/parse (subs s 0 sp)) (subs s (inc sp))]))

(defn pojo-decoder
  [ext paged-rdr pos]
  (let [n (bytes/to-number (read-bytes paged-rdr pos bytes/NUMBER_BYTES)) 
        b (read-bytes paged-rdr (+ pos bytes/NUMBER_BYTES) n)]
    (.parse js/JSON (bytes/to-string b))))

(defn default-decoder
  "This is a decoder for unsupported data that has a string constructor"
  [ext paged-rdr pos]
  (throw (ex-info "Not implemented" {})))

(def typecode->decoder
  "Map of type codes to decoder functions"
  {0 long-decoder
   1 double-decoder
   2 string-decoder
   3 uri-decoder
   ;; 6 bigint-decoder
   ;; 7 bigdec-decoder
   8 date-decoder
   9 instant-decoder
   10 keyword-decoder
   11 uuid-decoder
   12 blob-decoder
   13 xsd-decoder
   14 pojo-decoder})

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
