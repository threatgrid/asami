(ns ^{:doc "Tests the encoding/decoding operations"
      :author "Paula Gearon"}
    asami.durable.codec-test
  #?(:cljs (:refer-clojure :exclude [get]))
  (:require [asami.durable.codec :as codec]
            [asami.durable.encoder :as encoder :refer [to-bytes encapsulate-id]]
            [asami.durable.decoder :as decoder :refer [read-object unencapsulate-id decode-length-node]]
            [asami.durable.common :refer [Paged refresh! read-byte read-short read-bytes read-bytes-into
                                          FlatStore write-object! get-object force!]]
            #?(:clj [asami.durable.flat-file :refer [paged-file]])
            [clojure.string :as s]
            [clojure.test #?(:clj :refer :cljs :refer-macros) [deftest is]])
  #?(:clj
     (:import [java.io RandomAccessFile File]
              [java.nio ByteBuffer]
              [java.time Instant ZoneOffset]
              [java.util Date Arrays GregorianCalendar TimeZone]
              [java.net URI URL])
     :cljs
     (:import [goog Uri])))

#?(:cljs
   (defn byte-array
     [len] (js/ArrayBuffer. len)))

(defn byte-length [bytes]
  #?(:clj (alength bytes)
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

#?(:cljs (declare get))

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

(defn now [] #?(:clj (Date.) :cljs (js/Date.)))

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
    (rt (now))
    #?(:clj (rt (Instant/now)))
    (rt #uuid "1df3b523-357e-4339-a009-2d744e372d44")
    (rt (byte-array (range 256)))
    #?(:clj (rt (URL. "http://data.org/")))))

(deftest test-collections-codecs
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
             (let [r (read-object rdr 0)
                   eq (fn [a b]
                        (if (bytes? a)
                          (is (array-equals a b))
                          (is (= a b))))]
               (is (= (count r) (count o)))
               (is (every? identity (map eq r o)))))]
    (rt [])
    (rt ["hello"])
    (rt [:hello])
    (rt [(uri "http://hello.com/")])
    (rt [2020])
    (rt [20.20])
    (rt [(str-of-len 128)])
    (rt [(str-of-len 130)])
    (rt [(str-of-len 2000)])
    (rt [(uri (str "http://data.org/?ref=" (s/replace (str-of-len 100) "100 " "1234")))])
    (rt [:key45678901234567890123456789012345])
    (rt [123456789012345678901234567890N])
    (rt [12345678901234567890.0987654321M])
    (rt [(now)])
    #?(:clj (rt [(Instant/now)]))
    (rt [#uuid "1df3b523-357e-4339-a009-2d744e372d44"])
    (rt [(byte-array (range 256))])
    #?(:clj (rt [(URL. "http://data.org/")]))
    (rt [1 2 3])
    (rt (range 30))
    (rt ["The" "quick" "brown" "fox" "" "jumps" "over" "the" "lazy" "dog"])
    (rt ["hello" :hello 2020 20.20 (str-of-len 130) (uri "http://data.org/?ref=1234567890")
         123456789012345678901234567890N 12345678901234567890.0987654321M (now)
         #uuid "1df3b523-357e-4339-a009-2d744e372d44" (byte-array (range 256)) :end "end"])))

(deftest test-map-codecs
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
    (rt {})
    (rt {:a 1})
    (rt {:a 1 :b 2})
    (rt {:a "foo" "bar" 5})
    (rt {:a {:b 5 :c [6 7 8]} "b" ["9" 10]})))

(defn copy-to!
  [dest dest-offset src len]
  (doseq [n (range len)]
    (#?(:clj aset-byte :cljs aset) dest (+ dest-offset n) (aget src n))))

(deftest test-array-decoder
  (let [bb (byte-array 2048)
        write! (fn [o]
                 (let [[header body] (to-bytes o)
                       header-size (byte-length header)
                       body-size (byte-length body)]
                   (copy-to! bb 0 header header-size)
                   (copy-to! bb header-size body body-size)))
        rt (fn [o]
             (write! o)
             (let [len (decode-length-node bb)
                   olen (if (keyword? o) (dec (count (str o))) (count (str o)))]
               (is (= (if (> olen 23) 0x3F olen) len))))]
    (rt "hello")
    (rt :hello)
    (rt (uri "http://hello.com/"))
    (rt (str-of-len 128))
    (rt (str-of-len 130))
    (rt (str-of-len 2000))
    (rt (uri (str "http://data.org/?ref=" (s/replace (str-of-len 100) "100 " "1234"))))
    (rt :key45678901234567890123456789012345)))

;; this is an unsupported class with a string constructor

#?(:clj
   (deftest test-string-encapsulation
     (let [ess #(encoder/encapsulate-sstr % codec/sstr-type-mask)]
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
       (is (= (ess "abc🙂")   -0x189E9D9C0F60667E))  ;; 0xE7616263F09F9982
       (is (nil? (ess "abcd🙂"))))))

#?(:clj
   (deftest test-encapsulation
     (let [cal (doto (GregorianCalendar. 2021 0 20 12 0)
                 (.setTimeZone (TimeZone/getTimeZone ZoneOffset/UTC)))
           date (.getTime cal)
           inst (.toInstant cal)]
       (is (= (encapsulate-id "")        -0x2000000000000000)) ;; 0xE000000000000000
       (is (= (encapsulate-id "seven..") -0x188C9A899A91D1D2)) ;; 0xE7736576656E2E2E
       (is (nil? (encapsulate-id "eight...")))
       (is (= (encapsulate-id :a)        -0x6E9F000000000000)) ;; 0x9161000000000000
       (is (= (encapsulate-id :keyword)  -0x68949A8688908D9C)) ;; 0x976b6579776f7264
       (is (nil? (encapsulate-id :keywords)))
       (is (= (encapsulate-id date)      -0x3FFFFE88E0558E00)) ;; 0xC00001771FAA7200
       (is (= (encapsulate-id inst)      -0x5FFFFE88E0558E00)) ;; 0xA00001771FAA7200
       (is (nil? (encapsulate-id (.plusNanos inst 7))))
       (is (= (encapsulate-id true)      -0x4800000000000000)) ;; 0xB800000000000000
       (is (= (encapsulate-id false)     -0x5000000000000000)) ;; 0xB000000000000000
       (is (= (encapsulate-id 42)        -0x7FFFFFFFFFFFFFD6)) ;; 0x800000000000002A
       (is (= (encapsulate-id -42)       -0x700000000000002A)) ;; 0x8FFFFFFFFFFFFFD6
       ;; check some of the boundary conditions for long values
       (is (= (encapsulate-id 0x0400000000000000) -0x7C00000000000000)) ;; 0x8400000000000000
       (is (= (encapsulate-id -0x0400000000000000) -0x7400000000000000)) ;; 0x8C00000000000000
       (is (nil? (encapsulate-id 0x0800000000000000)))
       (is (nil? (encapsulate-id -0x0800000000000000)))
       (is (nil? (encapsulate-id 0x07FFFFFFFFFFFFFF)))
       (is (= (encapsulate-id  0x07FFFFFFFFFFFFFE) -0x7800000000000002)) ;; 0x87fffffffffffffe
       (is (= (encapsulate-id -0x07FFFFFFFFFFFFFF) -0x77ffffffffffffff)) ;; 0x8800000000000001
       (is (nil? (encapsulate-id -0x0800000000000000))))))

#?(:clj
   (deftest test-string-extract
     (is (= ""        (decoder/extract-sstr -0x2000000000000000)))   ;; 0xE000000000000000
     (is (= "a"       (decoder/extract-sstr -0x1E9F000000000000)))   ;; 0xE161000000000000
     (is (= "at"      (decoder/extract-sstr -0x1D9E8C0000000000)))   ;; 0xE261740000000000
     (is (= "one"     (decoder/extract-sstr -0x1C90919B00000000)))   ;; 0xE36F6E6500000000
     (is (= "four"    (decoder/extract-sstr -0x1B99908A8E000000)))   ;; 0xE4666F7572000000
     (is (= "fifth"   (decoder/extract-sstr -0x1A9996998B980000)))   ;; 0xE566696674680000
     (is (= "sixthy"  (decoder/extract-sstr -0x198C96878B978700)))   ;; 0xE673697874687900
     (is (= "seven.." (decoder/extract-sstr -0x188C9A899A91D1D2)))   ;; 0xE7736576656E2E2E
     (is (= "sevenΦ"  (decoder/extract-sstr -0x188C9A899A91315A))))) ;; 0xE7736576656ECEA6

#?(:clj
   (deftest test-extract
     (let [cal (doto (GregorianCalendar. 2021 0 20 12 0)
                 (.setTimeZone (TimeZone/getTimeZone ZoneOffset/UTC)))
           date (.getTime cal)
           inst (.toInstant cal)]
       (is (= ""        (unencapsulate-id -0x2000000000000000)))   ;; 0xE000000000000000
       (is (= "a"       (unencapsulate-id -0x1E9F000000000000)))   ;; 0xE161000000000000
       (is (= "at"      (unencapsulate-id -0x1D9E8C0000000000)))   ;; 0xE261740000000000
       (is (= "one"     (unencapsulate-id -0x1C90919B00000000)))   ;; 0xE36F6E6500000000
       (is (= "four"    (unencapsulate-id -0x1B99908A8E000000)))   ;; 0xE4666F7572000000
       (is (= "fifth"   (unencapsulate-id -0x1A9996998B980000)))   ;; 0xE566696674680000
       (is (= "sixthy"  (unencapsulate-id -0x198C96878B978700)))   ;; 0xE673697874687900
       (is (= "seven.." (unencapsulate-id -0x188C9A899A91D1D2)))   ;; 0xE7736576656E2E2E
       (is (= "sevenΦ"  (unencapsulate-id -0x188C9A899A91315A)))   ;; 0xE7736576656ECEA6
       (is (= :a (unencapsulate-id -0x6E9F000000000000)))
       (is (= :keyword (unencapsulate-id -0x68949A8688908D9C)))
       (is (= date (unencapsulate-id -0x3FFFFE88E0558E00)))
       (is (= inst (unencapsulate-id -0x5FFFFE88E0558E00)))
       (is (= true (unencapsulate-id codec/boolean-true-bits)))
       (is (= false (unencapsulate-id codec/boolean-false-bits)))
       (is (= 42 (unencapsulate-id -0x7FFFFFFFFFFFFFD6)))
       (is (= -42 (unencapsulate-id -0x700000000000002A)))
       (is (= 0x07FFFFFFFFFFFFFE (unencapsulate-id -0x7800000000000002)))
       (is (= -0x07FFFFFFFFFFFFFF (unencapsulate-id -0x77ffffffffffffff))))))

;; this test will work on CLJS, but on the JVM it is guaranteed to not use storage, and this is being tested
#?(:clj
   (deftest test-id-codec
     (let [cal (GregorianCalendar. 2021 0 20 12 0)
           date (.getTime cal)
           inst (.toInstant cal)
           check (fn [v] (is (= v (unencapsulate-id (encapsulate-id v)))))]
       (check "")
       (check "a")
       (check "at")
       (check "one")
       (check "four")
       (check "fifth")
       (check "sevenΦ")
       (check "abc🙂")
       (check :one)
       (check :keyword)
       (check date)
       (check inst)
       (check 0)
       (check -1)
       (check 42))))
