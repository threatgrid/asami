(ns ^{:doc "Tests the paging mechanism for reading flat files"
      :author "Paula Gearon"}
    asami.durable.pages-test
  (:require [asami.durable.encoder :refer [to-bytes]]
            [asami.durable.common :refer [read-byte read-short read-bytes]]
            [asami.durable.flat-file :refer [paged-file]]
            [clojure.test :refer [deftest is]])
  (:import [java.io RandomAccessFile File]))

(defn write-object [^RandomAccessFile f o]
  (let [[hdr data] (to-bytes o)]
    (.write f hdr)
    (.write f data)))

(deftest test-append
  ;; don't really need to test this with a file, but it's a useful template for other tests
  (let [f (File. "test-append.dat")]
    (.delete f)
    (with-open [of (RandomAccessFile. f "rw")]
      (let [buffer (byte-array 10)]
        (doto of
          (write-object "1234567890") ;; 1 + 10
          (write-object :keyword)     ;; 1 + 7
          (write-object 1023)         ;; 1 + 8
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
      (doto of
        (write-object "1234567890") ;; 1 + 10
        (write-object :keyword)     ;; 1 + 7
        (write-object 1023))        ;; 1 + 8
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
        (is (= "1234567890" (String. (read-bytes r 1 10) "UTF-8")))))
    (.delete f)))

(defn s-as-long [s] (bit-and 0xffff s))

(deftest test-straddle-read
  (let [f (File. "test-straddle.dat")]
    (.delete f)
    (with-open [of (RandomAccessFile. f "rw")]
      (doseq [n (range 10)]
        (doto of
          (write-object (format "123456789%d" n))    ;; 1 + 10
          (write-object (+ 0x123456789abcdef0 n))))  ;; 1 + 8
      
      (let [r (paged-file of 13)
            buffer (byte-array 10)]
        (doseq [n (range 10)]
          (let [offset (* n 20)]
            (is (= 0xa (read-byte r offset)))
            (is (= (format "123456789%d" n) (String. (read-bytes r (inc offset) buffer) "UTF-8")))
            (is (= 0xe0 (b-as-long (read-byte r (+ offset 11)))))
            (is (= 0x1234 (read-short r (+ offset 12))))
            (is (= 0x5678 (read-short r (+ offset 14))))
            (is (= 0x9abc (s-as-long (read-short r (+ offset 16)))))
            (is (= (+ 0xdef0 n) (s-as-long (read-short r (+ offset 18)))))))))
    (.delete f)))

(deftest test-expansion
  (let [f (File. "test-expand.dat")]
    (.delete f)
    (try
      (with-open [of (RandomAccessFile. f "rw")]
        (let [r (paged-file of 13)
              buffer (byte-array 10)]
          (doseq [n (range 10)]
            (doto of
              (write-object (format "123456789%x" n))   ;; 1 + 10
              (write-object (+ 0x123456789abcdef0 n)))) ;; 1 + 8
          (doseq [n (range 10)]
            (let [offset (* n 20)]
              (is (= 0xa (read-byte r offset)))
              (is (= (format "123456789%x" n) (String. (read-bytes r (inc offset) buffer) "UTF-8")))
              (is (= 0xe0 (b-as-long (read-byte r (+ offset 11)))))
              (is (= 0x1234 (read-short r (+ offset 12))))
              (is (= 0x5678 (read-short r (+ offset 14))))
              (is (= 0x9abc (s-as-long (read-short r (+ offset 16)))))
              (is (= (+ 0xdef0 n) (s-as-long (read-short r (+ offset 18)))))))
          (is (= 200 (.length of)))

          (doseq [n (range 10 16)]
            (doto of
              (write-object (format "123456789%x" n))   ;; 1 + 10
              (write-object (+ 0x123456789abcdef0 n)))) ;; 1 + 8
          (is (= 320 (.length of)))
          (doseq [n (shuffle (range 16))]
            (let [offset (* n 20)]
              (is (= 0xa (read-byte r offset)) (str "Failed for n=" n))
              (is (= (format "123456789%x" n) (String. (read-bytes r (inc offset) buffer) "UTF-8")) (str "Failed for n=" n))
              (is (= 0xe0 (b-as-long (read-byte r (+ offset 11)))))
              (is (= 0x1234 (read-short r (+ offset 12))))
              (is (= 0x5678 (read-short r (+ offset 14))))
              (is (= 0x9abc (s-as-long (read-short r (+ offset 16)))))
              (is (= (+ 0xdef0 n) (s-as-long (read-short r (+ offset 18)))))))))
      (finally
        (.delete f)))))
