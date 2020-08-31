(ns ^{:doc "Tests the paging mechanism for reading flat files"
      :author "Paula Gearon"}
    asami.durable.test-pages
  (:require [asami.durable.encoder :refer [to-bytes]]
            [asami.durable.pages :refer [refresh! read-byte read-short read-bytes]]
            [asami.durable.flat :refer [paged-file]]
            [clojure.test :refer [deftest is]])
  (:import [java.io RandomAccessFile File]))

(deftest test-append
  ;; don't really need to test this with a file, but it's a useful template for other tests
  (let [f (File. "test-append.dat")]
    (.delete f)
    (with-open [of (RandomAccessFile. f "rw")]
      (let [[sh sb] (to-bytes "1234567890")
            [kh kb] (to-bytes :keyword)
            [lh lb] (to-bytes 1023)
            buffer (byte-array 10)]
        (doto of
          (.write sh)  ;; 1
          (.write sb)  ;; 10
          (.write kh)  ;; 1 
          (.write kb)  ;; 7
          (.write lh)  ;; 1
          (.write lb)  ;; 8
          (.seek 0))
        (is (= 28 (.length of)))
        (is (= 0xa (.read of)))
        (is (= 10 (.read of buffer)))
        (is (= "1234567890" (String. buffer "UTF-8")))
        (is (= 0xc7 (.read of)))
        (is (= 7 (.read of buffer 0 7)))
        (is (= "keyword" (String. buffer 0 7 "UTF-8")))
        (is (= 0xe0 (.read of)))
        (is (= 1023 (.readLong of)))))
    (.delete f)))

(defn b-as-long [b] (bit-and 0xff b))

(deftest simple-read
  (let [f (File. "test-simple.dat")]
    (.delete f)
    (with-open [of (RandomAccessFile. f "rw")]
      (let [[sh sb] (to-bytes "1234567890")
            [kh kb] (to-bytes :keyword)
            [lh lb] (to-bytes 1023)]
        (doto of
          (.write sh)  ;; 1
          (.write sb)  ;; 10
          (.write kh)  ;; 1 
          (.write kb)  ;; 7
          (.write lh)  ;; 1
          (.write lb)  ;; 8
          (.seek 0))
        (let [r (paged-file of)
              bytes (byte-array 10)]
          (is (= 0xe0 (b-as-long (read-byte r 19))))
          (is (= 0xc7 (b-as-long (read-byte r 11))))
          (is (= 0x0a (b-as-long (read-byte r 0))))
          (is (= 0 (read-short r 20)))
          (is (= 0 (read-short r 22)))
          (is (= 0 (read-short r 24)))
          (is (= 0x03ff (read-short r 26)))
          (is (= "keyword" (String. (read-bytes r 12 7) "UTF-8")))
          (is (= "1234567890" (String. (read-bytes r 1 10) "UTF-8"))))))
    (.delete f)))
