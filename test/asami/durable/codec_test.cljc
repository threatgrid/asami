(ns ^{:doc "Tests the encoding/decoding operations"
      :author "Paula Gearon"}
    asami.durable.codec-test
  (:require [asami.durable.encoder :as encoder :refer [to-bytes encapsulate-sstr sstr-type-mask]]
            [asami.durable.decoder :as decoder :refer [read-object unencapsulate-id extract-long extract-sstr]]
            [asami.durable.common :refer [Paged refresh! read-byte read-short read-bytes read-bytes-into
                                          FlatStore write-object! get-object force!]]
            #?(:clj [asami.durable.flat-file :refer [paged-file]])
            [clojure.string :as s]
            [clojure.test :refer [deftest is]])
  #?(:clj
     (:import [java.io RandomAccessFile File]
              [java.nio ByteBuffer]
              [java.time Instant]
              [java.util Date Arrays]
              [java.net URI URL])
     :cljs
     (:import [goog Uri])))

#?(:cljs
   (defn byte-array
     [len] (js/ArrayBuffer. len)))

(defn byte-length [bytes]
  #?(:clj (count bytes)
     :cljs (or (.-byteLength bytes)
               (count bytes))))

#?(:cljs
   (defn bytes? [x]
     (or (instance? js/ArrayBuffer x)
         (instance? js/Int8Array x)
         (instance? js/Uint8Array x)
         (instance? js/Int8Array x)
         (instance? js/Uint8Array x)
         (instance? js/Uint8ClampedArray x)
         (instance? js/Int16Array x)
         (instance? js/Uint16Array x)
         (instance? js/Int32Array x)
         (instance? js/Uint32Array x)
         (instance? js/Float32Array x)
         (instance? js/Float64Array x)
         #_(instance? js/BigInt64Array x)
         #_(instance? js/BigUint64Array x))))

;; TODO: This likely needs to go in a separate file along with many
;; of the other helpers in this namespace to provide a platform
;; independent API.
#?(:cljs
   (deftype ByteBuffer [^js/DataView __dataView __position __limit __readOnly]
     Object
     (asReadOnlyBuffer [this]
       (ByteBuffer. __dataView __position __limit true))

     (get [this]
       (if (not (< __position __limit))
         (throw (ex-info "Buffer underflow" {:currentLimit __limit
                                             :currentPosition __position})))

       (let [byte (.getInt8 __dataView __position)]
         (set! (.-__position this) (inc __position))

         byte))

     (get [this number-or-destination]
       (if (number? number-or-destination)
         (.getInt8 __dataView number-or-destination)
         (get this number-or-destination 0 (byte-length number-or-destination))))

     (get [this destination offset length]
       (dotimes [i length]
         (aset destination (+ offset i) (.get this)))

       this)

     (getShort [this]
       (let [short (.getInt16 __dataView __position)]
         (set! (.-__position this) (+ 2 __position))
         short))

     (getShort [this offset]
       (let [short (.getInt16 __dataView offset)]
         short))

     (position [this]
       __position)

     (position [this newPosition]
       (if (neg? newPosition)
         (throw (ex-info "New position must be non-negative" {:newPosition newPosition})))
       
       (if (< __limit newPosition)
         (throw (ex-info "New position must be less that the current limit"
                         {:currentLimit __limit
                          :newPosition newPosition})))

       (set! (.-__position this) newPosition)

       this)

     (put [this byte-or-bytes]
       (if (seqable? byte-or-bytes)
         (.put this byte-or-bytes 0 (byte-length byte-or-bytes))
         (do (.setInt8 __dataView __position byte-or-bytes)
             (set! (.-__position this) (inc __position))
             this)))

     (put [this bytes offset length]
       (doseq [byte (take length (drop offset bytes))]
         (.setInt8 __dataView __position byte)
         (set! (.-__position this) (inc __position)))

       this)

     (putShort [this short]
       (.setInt16 __dataView __position short)
       (set! (.-__position this) (+ 2 __position))
       this)

     (putShort [this index short]
       (.setInt16 __dataView index short)
       this)))

(defn byte-buffer [byte-array]
  #?(:clj (ByteBuffer/wrap byte-array)
     :cljs (->ByteBuffer (js/DataView. byte-array) 0 (.-byteLength byte-array) false)))

(defrecord TestReader [b]
  Paged
  (refresh! [this])

  (read-byte [this offset]
    (.get b offset))

  (read-short [this offset]
    (.getShort b offset))

  (read-bytes [this offset len]
    #?(:clj (read-bytes-into this offset (byte-array len))
       :cljs (read-bytes-into this offset (js/Int8Array. (byte-array len)))))

  (read-bytes-into [this offset bytes]
    (.get (.position (.asReadOnlyBuffer b) offset) bytes 0 (byte-length bytes))
    bytes))

(defn str-of-len
  [n]
  (let [ten "1234567890"
        h (str n \space)
        start (subs ten (count h))]
    (str (apply str h start (repeat (dec (/ n 10)) ten))
         (subs ten 0 (mod n 10)))))

(defn uri
  [s]
  #?(:clj (URI. s)
     :cljs (goog/Uri. s)))

(defn array-equals
  [a b]
  #?(:clj (Arrays/equals a b)
     :cljs (and (= (count a) (count b))
                (every? identity (map = a b)))))

(deftest test-codecs
  (let [bb (byte-buffer (byte-array 2048))
        rdr (->TestReader bb)
        write! (fn [o]
                 (let [[header body] (to-bytes o)
                       header-size (byte-length header)
                       body-size (byte-length body)]
                   (.position bb 0)
                   (.put bb header 0 header-size)
                   (.put bb body 0 body-size)))
        rt (fn [o]
             (write! o)
             (let [r (read-object rdr 0)]
               (if (bytes? o)
                 (is (array-equals o r))
                 (is (= o r)))))]
    (rt "hello")
    (rt :hello)
    (rt (uri "http://hello.com/"))
    (rt 2020)
    (rt 20.20)
    (rt (str-of-len 128))
    (rt (str-of-len 130))
    (rt (str-of-len 2000))
    (rt (uri (str "http://data.org/?ref=" (s/replace (str-of-len 100) "100 " "1234"))))
    (rt :key45678901234567890123456789012345)
    (rt 123456789012345678901234567890N)
    (rt 12345678901234567890.0987654321M)
    #?(:clj (rt (Date.))
       :cljs (rt (js/Date.)))
    #?(:clj (rt (Instant/now)))
    (rt #uuid "1df3b523-357e-4339-a009-2d744e372d44")
    (rt (byte-array (range 256)))
    #?(:clj (rt (URL. "http://data.org/")))))

;; this is an unsupported class with a string constructor

#?(:clj
   (deftest test-encapsulation
     (let [ess #(encapsulate-sstr % sstr-type-mask)]
       (is (= (ess "")        -0x2000000000000000))  ;; 0xE000000000000000
       (is (= (ess "a")       -0x1E9F000000000000))  ;; 0xE161000000000000
       (is (= (ess "at")      -0x1D9E8C0000000000))  ;; 0xE261740000000000
       (is (= (ess "one")     -0x1C90919B00000000))  ;; 0xE36F6E6500000000
       (is (= (ess "four")    -0x1B99908A8E000000))  ;; 0xE4666F7572000000
       (is (= (ess "fifth")   -0x1A9996998B980000))  ;; 0xE566696674680000
       (is (= (ess "sixthy")  -0x198C96878B978700))  ;; 0xE673697874687900
       (is (= (ess "seven..") -0x188C9A899A91D1D2))  ;; 0xE7736576656E2E2E
       (is (nil? (ess "eight...")))
       (is (= (ess "sevenΦ")  -0x188C9A899A91315A))  ;; 0xE7736576656ECEA6
       (is (nil? (ess "eight.Φ")))
       )))

#?(:clj
   (deftest test-extract
     (is (= ""        (extract-sstr -0x2000000000000000)))   ;; 0xE000000000000000
     (is (= "a"       (extract-sstr -0x1E9F000000000000)))   ;; 0xE161000000000000
     (is (= "at"      (extract-sstr -0x1D9E8C0000000000)))   ;; 0xE261740000000000
     (is (= "one"     (extract-sstr -0x1C90919B00000000)))   ;; 0xE36F6E6500000000
     (is (= "four"    (extract-sstr -0x1B99908A8E000000)))   ;; 0xE4666F7572000000
     (is (= "fifth"   (extract-sstr -0x1A9996998B980000)))   ;; 0xE566696674680000
     (is (= "sixthy"  (extract-sstr -0x198C96878B978700)))   ;; 0xE673697874687900
     (is (= "seven.." (extract-sstr -0x188C9A899A91D1D2)))   ;; 0xE7736576656E2E2E
     (is (= "sevenΦ"  (extract-sstr -0x188C9A899A91315A)))   ;; 0xE7736576656ECEA6
     ))
