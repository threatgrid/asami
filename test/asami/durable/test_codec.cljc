(ns ^{:doc "Tests the encoding/decoding operations"
      :author "Paula Gearon"}
    asami.durable.test-codec
  (:require [asami.durable.encoder :refer [to-bytes]]
            [asami.durable.decoder :refer [read-object]]
            [asami.durable.pages :refer [Paged refresh! read-byte read-short read-bytes read-bytes-into]]
            [asami.durable.flat :refer [FlatStore write-object! get-object force!]]
            [asami.durable.flat-file :refer [paged-file]]
            [clojure.string :as s]
            [clojure.test :refer [deftest is]])
  #?(:clj
     (:import [java.io RandomAccessFile File]
              [java.nio ByteBuffer]
              [java.time Instant]
              [java.util Date Arrays]
              [java.net URI URL])))

#?(:cljs
   (defn byte-array
     [len]
     ;; todo: return a byte-array
     ))

#?(:clj
   (defrecord TestReader [b]
     Paged
     (refresh! [this])
     (read-byte [this offset] (.get b offset))
     (read-short [this offset] (.getShort b offset))
     (read-bytes [this offset len] (read-bytes-into this offset (byte-array len)))
     (read-bytes-into [this offset bytes]
       (.get (.position (.asReadOnlyBuffer b) offset) bytes 0 (count bytes))
       bytes))

   :cljs
   (defrecord TestReader [b]
     Paged
     (refresh! [this])
     (read-byte [this offset] )
     (read-short [this offset] )
     (read-bytes [this offset len] (read-bytes-into this offset (byte-array len)))
     (read-bytes-into [this offset bytes] bytes)))

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

#?(:clj
   ;; The following is being skipped in cljs until implemented
   (deftest test-codecs
     (let [bb (ByteBuffer/wrap (byte-array 2048))
           rdr (->TestReader bb)
           write! (fn [o]
                    (let [[hdr data] (to-bytes o)]
                      (.position bb 0)
                      (.put bb hdr 0 (count hdr))
                      (.put bb data 0 (count data)))
                    
                    )
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
       #?(:clj (rt (URL. "http://data.org/"))))))  ;; this is an unsupported class with a string constructor
