(ns ^{:doc "Encodes and decodes data for storage. Clojure implementation"
      :author "Joel Holdbrooks"}
    asami.durable.decoder
  (:require [clojure.string :as s]
            [asami.durable.common :refer [read-byte read-bytes read-short]])
  (:import [goog.math Long Integer]
           [goog Uri]))

(def ^{:private true}
  BYTE_BYTES 1)

(def ^{:private true}
  SHORT_BYTES 1)

(def ^{:private true}
  SHORT_SIZE 16)

(def ^{:private true}
  INTEGER_BYTES 4)

(def ^{:private true}
  LONG_BYTES 8)

;; temporary stub
(defn type-info [data] 0)

(defn decode-length
  "Reads the header to determine length.
  ext: if 0 then length is a byte, if 1 then length is in either a short or an int"
  [ext paged-rdr ^long pos]
  (if ext
    (let [raw (read-byte paged-rdr pos)]
      [BYTE_BYTES (bit-and 0xFF raw)])
    (let [len (read-short paged-rdr pos)]
      (if (< len 0)
        (let [len2 (read-short paged-rdr pos)]
          [INTEGER_BYTES (bit-or
                          (bit-shift-left (int (bit-and 0x7FFF len)) SHORT_SIZE)
                          len2)])
        [SHORT_BYTES len]))))

;; Readers are given the length and a position. They then read data into a type

(defn bytes->str [bytes]
  (let [decoder (js/TextDecoder.)]
    (.decode decoder bytes)))

(defn read-str
  [paged-rdr ^long pos ^long len]
  (bytes->str (read-bytes paged-rdr pos len)))

(defn read-uri
  [paged-rdr ^long pos ^long len]
  (Uri/parse (read-str paged-rdr pos len)))

(defn read-keyword
  [paged-rdr ^long pos ^long len]
  (keyword (read-str paged-rdr pos len)))

;; decoders operate on the bytes following the initial type byte information
;; if the data type has variable length, then this is decoded first

(defn bytes->int
  {:private true}
  [bytes] ;; `bytes` is assumed to be a `js/Array` like object.
  (let [bytes (to-array bytes)]
    (bit-or (bit-shift-left (bit-and (aget bytes 0) 0xFF) 24)
            (bit-shift-left (bit-and (aget bytes 1) 0xFF) 16)
            (bit-shift-left (bit-and (aget bytes 2) 0xFF) 8)
            (bit-shift-left (bit-and (aget bytes 3) 0xFF) 0))))

(defn bytes->long
  [bytes] ;; `bytes` is assumed to be an `js/Array` like object.
  (let [high-bits (bytes->int (.slice bytes 0 4))
        low-bits (bytes->int (.slice bytes 4 8))]
    (.fromBits Long low-bits high-bits)))

(defn long-decoder
  [ext paged-rdr ^long pos]
  (bytes->long (read-bytes paged-rdr pos LONG_BYTES)))

#_
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

(def ^:const instant-length (+ LONG_BYTES INTEGER_BYTES))

(defn instant-decoder
  [ext paged-rdr ^long pos]
  (let [b (read-bytes paged-rdr pos instant-length)
        ;; epoch (.getLong b 0) ;; Ignored for now.
        sec (bytes->long b)]
    (js/Date. (* 1000 sec))))

(defn keyword-decoder
  [ext paged-rdr ^long pos]
  (let [[i len] (decode-length ext paged-rdr pos)]
    (read-keyword paged-rdr (+ pos i) len)))

(def ^:const uuid-length
  (* 2 LONG_BYTES))

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

#_
(defn blob-decoder
  [ext paged-rdr ^long pos]
  (let [[i len] (decode-length ext paged-rdr pos)]
    (read-bytes paged-rdr (+ i pos) len)))

#_
(defn xsd-decoder
  [ext paged-rdr ^long pos]
  (let [s (string-decoder ext paged-rdr pos)
        sp (s/index-of s \space)]
    [(URI/create (subs s 0 sp)) (subs (inc sp))]))

(defn default-decoder
  "This is a decoder for unsupported data that has a string constructor"
  [ext paged-rdr ^long pos]
  (throw (ex-info "Not implemented" {}))
  #_
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
   ;; 1 double-decoder
   2 string-decoder
   3 uri-decoder
   ;; 6 bigint-decoder
   ;; 7 bigdec-decoder
   8 date-decoder
   9 instant-decoder
   10 keyword-decoder
   11 uuid-decoder
   ;; 12 blob-decoder
   ;; 13 xsd-decoder
   })

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
