(ns ^{:doc "Encodes and decodes data for storage. Clojure implementation"
      :author "Paula Gearon"}
    asami.durable.decoder
    (:require [clojure.string :as s]
              [asami.graph :as graph]
              [asami.durable.common :refer [read-byte read-bytes read-short]]
              [asami.durable.codec :refer [byte-mask data-mask sbytes-shift len-nybble-shift utf8
                                           long-type-code date-type-code inst-type-code
                                           sstr-type-code skey-type-code node-type-code]])
    (:import [clojure.lang Keyword BigInt]
             [java.math BigInteger BigDecimal]
             [java.net URI]
             [java.time Instant]
             [java.util Date UUID]
             [java.nio ByteBuffer]))


(defn decode-length
  "Reads the header to determine length.
  ext: if true (bit is 0) then length is a byte, if false (bit is 1) then length is in either a short or an int
  pos: The beginning of the data. This has skipped the type byte.
  returns: a pair of the header length and the data length."
  [ext paged-rdr ^long pos]
  (if ext
    [Byte/BYTES (bit-and 0xFF (read-byte paged-rdr pos))]
    (let [len (read-short paged-rdr pos)]
      (if (< len 0)
        (let [len2 (read-short paged-rdr pos)]
          [Integer/BYTES (bit-or
                          (bit-shift-left (int (bit-and 0x7FFF len)) Short/SIZE)
                          len2)])
        [Short/BYTES len]))))

(defn decode-length-node
  "Reads the header to determine length.
  data: The complete buffer to decode, including the type byte.
  returns: the length, or a lower bound on the length"
  [^bytes data]
  (let [b0 (aget data 0)]
    (cond ;; test for short format objects
      (zero? (bit-and 0x80 b0)) b0
      (zero? (bit-and 0x40 b0)) (bit-and 0x3F b0)
      (zero? (bit-and 0x20 b0)) (bit-and 0x1F b0)
      ;; First byte contains only the type information. Give a large number = 63
      :default 0x3F)))

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

(def ^:const nybble-mask 0xF)
(def ^:const long-nbit  0x0800000000000000)
(def ^:const lneg-bits -0x1000000000000000) ;; 0xF000000000000000

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

(defn extract-node
  [id]
  (asami.graph.InternalNode. (bit-and data-mask id)))

(defn unencapsulate-id
  "Converts an encapsulating ID into the object it encapsulates. Return nil if it does not encapsulate anything."
  [^long id]
  (when (> 0 id)
    (let [tb (bit-and (bit-shift-right id type-nybble-shift) nybble-mask)]
      (case tb
        0x8 (extract-long id)                          ;; long-type-code
        0xC (Date. (extract-long id))                  ;; date-type-code
        0xA (Instant/ofEpochMilli (extract-long id))   ;; inst-type-code
        0xE (extract-sstr id)                          ;; sstr-type-code
        0x9 (keyword (extract-sstr id))                ;; skey-type-code
        0xD (extract-node id)                          ;; node-type-code
        nil))))

(defn encapsulated-node?
  [^long id]
  (let [top-nb (bit-and (bit-shift-right id type-nybble-shift) nybble-mask)]
    (or (= top-nb skey-type-code) (= top-nb node-type-code))))

(defn type-info
  "Returns the type information encoded in a header-byte"
  [b]
  (cond
    (zero? (bit-and 0x80 b)) 2   ;; string
    (zero? (bit-and 0x40 b)) 3   ;; uri
    (zero? (bit-and 0x20 b)) 10  ;; keyword
    :default (let [tn (bit-and 0xF b)]
               (if (or (= tn 4) (= tn 5)) 3 tn))))

(defn partials-len
  "Determine the number of bytes that form a partial character at the end of a UTF-8 byte array.
  The len argument is the defined length of the full string, but that may be greater than the bytes provided."
  ([bs] (partials-len bs (alength bs)))
  ([bs len]
   (let [end (dec (min len (alength bs)))]
     (when (>= end 0)
       (loop [t 0]
         (if (= 4 t)  ;; Safety limit. Should not happen for well formed UTF-8
           t
           (let [b (aget bs (- end t))]
             (if (zero? (bit-and 0x80 b))  ;; single char that can be included
               t
               (if (zero? (bit-and 0x40 b))  ;; extension char that may be truncated
                 (recur (inc t))
                 (cond
                   (= 0xC0 (bit-and 0xE0 b)) (if (= 1 t) 0 (inc t)) ;; 2 bytes
                   (= 0xE0 (bit-and 0xF0 b)) (if (= 2 t) 0 (inc t)) ;; 3 bytes
                   (= 0xF0 (bit-and 0xF8 b)) (if (= 3 t) 0 (inc t)) ;; 4 bytes
                   :default (recur (inc t))))))))))))  ;; this should not happen for well formed UTF-8

(defn string-style-compare
  "Compare the string form of an object with bytes that store the string form of an object"
  [left-s right-bytes]
  (let [rbc (alength right-bytes) ;; length of all bytes
        full-length (decode-length-node right-bytes)
        ;; get the length of the bytes used in the string
        rlen (min full-length (dec rbc))
        ;; look for partial chars to be truncated, starting at the end.
        ;; string starts 1 byte in, after the header, so start at inc of the string byte length
        trunc-len (partials-len right-bytes (inc rlen))
        right-s (String. right-bytes 1 (- rlen trunc-len) utf8)
        ;; only truncate the LHS if the node does not contain all of the string data
        left-side (if (<= full-length (dec rbc))
                    left-s
                    (subs left-s 0 (min (count left-s) (count right-s))))]
    (compare left-side right-s)))

(defn long-bytes-compare
  "Compare data from 2 values that are the same type. If the data cannot give a result
   then return 0. Operates on an array, expected to be in an index node."
  [type-left left-header left-body left-object right-bytes]
  (case type-left
    2 (string-style-compare left-object right-bytes)   ;; String
    3 (string-style-compare (str left-object) right-bytes)  ;; URI
    10 (string-style-compare (subs (str left-object) 1) right-bytes)  ;; Keyword
    ;; otherwise, skip the type byte in the right-bytes, and raw compare left bytes to right bytes
    (or (first (drop-while zero? (map compare left-body (drop 1 right-bytes)))) 0)))

(defn read-object
  "Reads an object from a paged-reader, at id=pos"
  [paged-rdr ^long pos]
  (let [b0 (read-byte paged-rdr pos)
        ipos (inc pos)]
    (cond  ;; test for short format objects
      (zero? (bit-and 0x80 b0)) (read-str paged-rdr ipos b0)
      (zero? (bit-and 0x40 b0)) (read-uri paged-rdr ipos (bit-and 0x3F b0))
      (zero? (bit-and 0x20 b0)) (read-keyword paged-rdr ipos (bit-and 0x1F b0))
      ;; First byte contains only the type information
      :default ((typecode->decoder (bit-and 0x0F b0) default-decoder)
                (zero? (bit-and 0x10 b0)) paged-rdr ipos))))
