(ns ^{:doc "Encodes and decodes data for storage. Clojure implementation"
      :author "Paula Gearon"}
    asami.durable.encoder
    (:require [clojure.string :as s]
              [asami.graph :as graph]
              [asami.durable.codec :refer [byte-mask data-mask sbytes-shift len-nybble-shift utf8
                                           long-type-mask date-type-mask inst-type-mask
                                           sstr-type-mask skey-type-mask node-type-mask
                                           boolean-true-bits boolean-false-bits]])
    (:import [clojure.lang Keyword BigInt ISeq
                           IPersistentMap IPersistentVector PersistentVector
                           PersistentHashMap PersistentArrayMap PersistentTreeMap]
             [java.io RandomAccessFile]
             [java.math BigInteger BigDecimal]
             [java.net URI]
             [java.time Instant]
             [java.util Date UUID]
             [java.nio ByteBuffer]
             [java.nio.charset Charset]))

;; Refer to asami.durable.codec for a description of the encoding implemented below

;; (set! *warn-on-reflection* true)

(def ^:dynamic *entity-offsets* nil)
(def ^:dynamic *current-offset* nil)

(def empty-bytes (byte-array 0))

(def type->code
  {Long (byte 0)
   Double (byte 1)
   String (byte 2)
   URI (byte 3)
   ISeq (byte 4)
   IPersistentMap (byte 5)
   BigInt (byte 6)
   BigInteger (byte 6)
   BigDecimal (byte 7)
   Date (byte 8)
   Instant (byte 9)
   Keyword (byte 10)
   UUID (byte 11)
   :blob (byte 12)
   :xsd (byte 13)
   :pojo (byte 14)
   :internal (byte 15)})

(def
  registered-xsd-types
  "A map of types to encoding functions"
  (atom {}))

(defn str-constructor?
  "Tests if a class has a constructor that takes a string"
  [c]
  (try
    (boolean (.getConstructor ^Class c (into-array Class [String])))
    (catch Throwable _ false)))

(defn type-code
  "Returns a code number for an object"
  [o]
  (if (bytes? o)
    [(type->code :blob) identity]
    (if-let [encoder (get @registered-xsd-types (type o))]
      [(type->code :xsd) encoder]
      (if (str-constructor? (type o))
        [(type->code :pojo) (fn [obj] (.getBytes (str (.getName ^Class (type o)) " " obj) ^Charset utf8))]
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

;; to-counted-bytes is required by the recursive concattenation operation
(declare to-counted-bytes)

(defn concat-bytes
  "Takes multiple byte arrays and returns an array with all of the bytes concattenated"
  ^bytes [bas]
  (let [len (apply + (map alength bas))
        output (byte-array len)]
    (reduce (fn [offset arr]
              (let [l (alength arr)]
                (System/arraycopy arr 0 output offset l)
                (+ offset l)))
            0 bas)
    output))

(def zero-array
  "A single element byte array containing 0"
  (byte-array [(byte 0)]))

(defprotocol FlatFile
  (header [this len] "Returns a byte array containing a header")
  (body [this] "Returns a byte array containing the encoded data")
  (encapsulate-id [this] "Returns an ID that encapsulates the type"))

(def ^:const max-short-long  0x07FFFFFFFFFFFFFF)
(def ^:const min-short-long -0x0800000000000000) ;; 0xF800000000000000

(def ^:const max-short-len 7)

(def ^:const milli-nano "Number of nanoseconds in a millisecond" 1000000)

(defn encapsulate-sstr
  "Encapsulates a short string. If the string cannot be encapsulated, then return nil."
  [^String s type-mask]
  (when (<= (.length s) max-short-len)
    (let [abytes (.getBytes s ^Charset utf8)
          len (alength abytes)]
      (when (<= len max-short-len)
        (reduce (fn [v n]
                  (-> (aget abytes n)
                      (bit-and byte-mask)
                      (bit-shift-left (- sbytes-shift (* Byte/SIZE n)))
                      (bit-or v)))
                ;; start with the top byte set to the type nybble and the length
                (bit-or type-mask (bit-shift-left len len-nybble-shift))
                (range len))))))

(defn encapsulate-long
  "Encapsulates a long value. If the long is too large to be encapsulated, then return nil."
  [^long l]
  (when (and (< l max-short-long) (> l min-short-long))
    (bit-and data-mask l)))

(def ^:dynamic *number-bytes* nil)
(def ^:dynamic *number-buffer* nil)

(defn n-byte-number
  "Returns an array of n bytes representing the number x.
  Must be initialized for the current thread."
  [^long n ^long x]
  (.putLong *number-buffer* 0 x)
  (let [ret (byte-array n)]
    (System/arraycopy ^bytes *number-bytes* (int (- Long/BYTES n)) ret 0 (int n))
    ret))

(defn num-bytes
  "Determines the number of bytes that can hold a value.
  From 2-4 tests, this preferences small numbers."
  [^long n]
  (let [f (neg? n)
        nn (if f (dec (- n)) n)]
    (if (<= nn 0x7FFF)
      (if (<= nn 0x7F) 1 2)
      (if (<= nn 0x7FFFFFFF)
        (if (<= nn 0x7FFFFF) 3 4)
        (if (<= nn 0x7FFFFFFFFFFF)
          (if (<= nn 0x7FFFFFFFFF) 5 6)
          (if (<= nn 0x7FFFFFFFFFFFFF) 7 8))))))

(def constant-length?
  "The set of types that can be encoded in a constant number of bytes. Used for homogenous sequences."
  #{Long Double Date Instant UUID})


(defn map-header
  "Common function for encoding the header of the various map types"
  [this len]
  (general-header (type->code IPersistentMap) len))

(defn map-body
  "Common function for encoding the body of the various map types"
  [this]
  ;; If this is an identified object, then save its location
  (doseq [id-attr [:db/id :db/ident :id]]
    (when-let [id (id-attr this)]
      (vswap! *entity-offsets* assoc id @*current-offset*)))
  (body (apply concat (seq this))))

(extend-protocol FlatFile
  String
  (header [this len]
    (if (< len 0x80)
      (byte-array [len])
      (general-header (type->code String) len)))
  (body [^String this]
    (.getBytes this ^Charset utf8))
  (encapsulate-id [this]
    (encapsulate-sstr this sstr-type-mask))

  Boolean
  (header [this len]
    (throw (ex-info "Unexpected encoding of internal node" {:node this})))
  (body [this]
    (throw (ex-info "Unexpected encoding of internal node" {:node this})))
  (encapsulate-id [this]
    (if this boolean-true-bits boolean-false-bits))

  URI
  (header [this len]
    (if (< len 0x40)
      (byte-array [(bit-or 0x80 len)])
      (general-header (type->code URI) len)))
  (body [this]
    (.getBytes (str this) ^Charset utf8))
  (encapsulate-id [this] nil)
  
  Keyword
  (header [this len]
    (if (< len 0x10)
      (byte-array [(bit-or 0xC0 len)])
      (general-header (type->code Keyword) len)))
  (body [this]
    (.getBytes (subs (str this) 1) ^Charset utf8))
  (encapsulate-id [this]
    (encapsulate-sstr (subs (str this) 1) skey-type-mask))
  
  Long
  (header [this len]
    (assert (<= len Long/BYTES))
    (byte-array [(bit-or 0xD0 len)]))
  (body [^long this]
    (let [n (num-bytes this)]
      (n-byte-number n this)))
  (encapsulate-id [this]
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
  (encapsulate-id [this] nil)

  BigInt
  (header [this len]
    (general-header (type->code BigInt) len))
  (body [this]
    (.toByteArray (biginteger this)))
  (encapsulate-id [this] nil)
  
  BigDecimal
  (header [this len]
    (general-header (type->code BigDecimal) len))
  (body [^BigDecimal this]
    (.getBytes (str this) ^Charset utf8))
  (encapsulate-id [this] nil)

  Date
  (header [this len]
    (assert (= len Long/BYTES))
    (byte-array [(bit-or 0xE0 (type->code Date))]))
  (body [^Date this]
    (n-byte-number Long/BYTES (.getTime this)))
  (encapsulate-id [this]
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
  (encapsulate-id [this]
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
  (encapsulate-id [this] nil)

  ISeq
  (header [this len]
    (general-header (type->code ISeq) len))
  (body [this]
    (if-not (seq this)
      empty-bytes
      (let [fst (first this)
            t (type fst)
            homogeneous (and (constant-length? t) (every? #(instance? t %) this))
            [elt-fn prefix] (if homogeneous
                              (if (= t Long)
                                (let [elt-len (apply max (map num-bytes this))
                                      arr-hdr (byte-array [(bit-or 0xD0 elt-len)])]         ;; 0xDllll is the header byte for longs
                                  ;; integer homogenous arrays store the number in the header, with nil bodies
                                  [#(vector (n-byte-number elt-len %)) arr-hdr])
                                (let [arr-hdr (byte-array [(bit-or 0xE0 (type->code t))])]  ;; 0xEtttt is the header byte for typed things
                                  ;; simple homogenous arrays store everything in the object header, with nil bodies
                                  [#(vector (body %)) arr-hdr]))
                              [to-counted-bytes zero-array])
            ;; start counting the bytes that are going into the buffer
            starting-offset @*current-offset*
            _ (vswap! *current-offset* + 3)  ;; 2 bytes for a short header + 1 byte for the prefix array
            result (->> this
                        ;; like a mapv but records the lengths of the data as it iterates through the seq
                        (reduce (fn [arrays x]
                                  (let [offset @*current-offset* ;; save the start, as the embedded objects will update this
                                        [head body] (elt-fn x)]
                                    ;; regardless of what embedded objects have update the *current-offset* to, change it to the
                                    ;; start of the current object, plus its total size
                                    (vreset! *current-offset* (+ offset (alength head) (if body (alength body) 0)))
                                    ;; add the bytes of this object to the overall result of byte arrays
                                    (cond-> (conj! arrays head)
                                      body (conj! body))))  ;; only add the body if there is one
                                (transient [prefix]))
                        persistent!
                        concat-bytes)
            update-lengths (fn [m u]
                             (into {} (map (fn [[k v :as kv]]
                                             (if (> v starting-offset) [k (+ v u)] kv))
                                           m)))
            rlen (alength result)]
        ;; correct offsets for longer headers
        (cond
          (> rlen 0x7FFF) (vswap! *entity-offsets* update-lengths 3) ;; total 5 after the 2 already added
          (> rlen 0xFF) (vswap! *entity-offsets* update-lengths 1))  ;; total 3 after the 2 already added
        result)))
  (encapsulate-id [this] nil)

  PersistentVector
  (header [this len] (header (or (seq this) '()) len))
  (body [this] (body (or (seq this) '())))
  (encapsulate-id [this] nil)
  
  PersistentArrayMap
  (header [this len] (map-header this len))
  (body [this] (map-body this))
  (encapsulate-id [this] nil)
  
  PersistentHashMap
  (header [this len] (map-header this len))
  (body [this] (map-body this))
  (encapsulate-id [this] nil)
  
  PersistentTreeMap
  (header [this len] (map-header this len))
  (body [this] (map-body this))
  (encapsulate-id [this] nil)
  
  Object
  (header [this len]
    (let [tc (or (type->code (type this))
                 (first (type-code this)))]
      (general-header tc len)))
  (body [this]
    (if-let [tc (type->code (type this))]
      (.getBytes (str this) ^Charset utf8)
      (if-let [[_ encoder] (type-code this)]
        (encoder this))))
  (encapsulate-id [this] nil)

  asami.graph.InternalNode
  (header [this len]
    (throw (ex-info "Unexpected encoding of internal node" {:node this})))
  (body [this]
    (throw (ex-info "Unexpected encoding of internal node" {:node this})))
  (encapsulate-id [^asami.graph.InternalNode this]
    (bit-or node-type-mask (bit-and data-mask (.id this)))))

(defn to-counted-bytes
  "Returns a tuple of byte arrays, representing the header and the body"
  [o]
  (let [^bytes b (body o)]
    [(header o (alength b)) b]))

(defn to-bytes
  "Returns a tuple of byte arrays, representing the header and the body"
  [o]
  (binding [*entity-offsets* (volatile! {})
            *current-offset* (volatile! 0)
            *number-bytes* (byte-array Long/BYTES)]
    (binding [*number-buffer* (ByteBuffer/wrap *number-bytes*)]
      (conj (to-counted-bytes o) @*entity-offsets*))))
