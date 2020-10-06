(ns ^{:doc "Tests for the ManagedBlockFile implementation"
      :author "Paula Gearon"}
    asami.durable.block.blockmanager-test
  (:require [clojure.test :refer :all]
            [asami.durable.block.block-api :refer :all]
            [asami.durable.block.file.block-file :refer :all]
            [asami.durable.block.file.voodoo :as voodoo]
            [asami.durable.block.file.util :as util]
            [asami.durable.block.blockfile-test :refer [put-string! get-string str0 str1 str2 str3]])
  #?(:clj (:import [java.io File])))

(def test-block-size 256)

(defn cleanup
  "A windows hack that attempts to prompt the JVM into unmapping unreferenced files"
  []
  #?(:clj
     (when voodoo/windows?
       (System/gc)
       (System/runFinalization))))

(defn get-filename
  "Returns the resource for creating a manager.
  For Java, this is a java.io.File. On JS this is a string."
  [s]
  #?(:clj (let [f (util/temp-file s)]
            (doto ^File f .delete))
     :cljs s))

(defn create-block-manager
  "Central BlockManager construction. On the JVM this is ManagedBlockFile.
  Takes a string argument (for construction), and block size."
  [s sz]
  (let [f (get-filename s)]
    [f
     #?(:clj (create-managed-block-file f sz))]))

(defn recreate-block-manager
  "Central BlockManager construction. On the JVM this is ManagedBlockFile.
  Takes an argument for creating the manager (e.g. File, or string), and a size."
  [f sz]
  #?(:clj (create-managed-block-file f sz)))

(defn remove-file
  "Remove block manager resources."
  [f]
  #?(:clj (.delete f)))


(deftest test-allocate
  (let [[filename mbf] (create-block-manager "alloc" test-block-size)]
    (try
      (let [blk (allocate-block! mbf)]
        (is (not (nil? blk))))
      (finally
        (close mbf)
        (cleanup)
        (remove-file filename)))))

(deftest test-write
  (let [[filename mbf] (create-block-manager "mbwrite" test-block-size)
        ids (volatile! nil)]
    (try
      (let [b0 (allocate-block! mbf)
            id0 (get-id b0)
            _ (put-string! b0 str0)
            b3 (allocate-block! mbf)
            id3 (get-id b3)
            _ (put-string! b3 str3)
            b2 (allocate-block! mbf)
            id2 (get-id b2)
            _ (put-string! b2 str2)
            b1 (allocate-block! mbf)
            id1 (get-id b1)
            _ (put-string! b1 str1)]
        (vreset! ids [id0 id1 id2 id3])
        
        (is (= str2 (get-string (get-block mbf id2))))
        (is (= str0 (get-string (get-block mbf id0))))
        (is (= str1 (get-string (get-block mbf id1))))
        (is (= str3 (get-string (get-block mbf id3)))))
      (finally
        (close mbf)))
    
    ;; close all, and start again
    (cleanup)
    (try
      (let [[id0 id1 id2 id3] @ids
            mbf2 (recreate-block-manager filename test-block-size)]
        
        (try
          #?(:clj
             ;; did it persist?
             (is (= 4 (get-nr-blocks (:block-file @(:state mbf2))))))
          
          (is (= str2 (get-string (get-block mbf2 id2))))
          (is (= str0 (get-string (get-block mbf2 id0))))
          (is (= str1 (get-string (get-block mbf2 id1))))
          (is (= str3 (get-string (get-block mbf2 id3))))
          (finally
            (close mbf2))))
      (finally
        (cleanup)
        (remove-file filename)))))

(deftest test-performance
  (let [[filename mbf] (create-block-manager "perftest" Long/BYTES)
        nr-blocks 100000]

    ;; put numbers in the first 100,000 blocks
    (try
      (let [rand-mem (reduce (fn [m i]
                               (let [n (long (rand nr-blocks))
                                     b (allocate-block! mbf)]
                                 (put-long! b 0 n)
                                 (write-block! mbf b)
                                 (assoc m (get-id b) n)))
                             {} (range nr-blocks))
            ;; commit the first 100,000
            mbf (commit! mbf)
            ;; put numbers in the next 100,000 blocks
            rand-mem2 (reduce (fn [m i]
                                (let [n (long (rand nr-blocks))
                                      b (allocate-block! mbf)]
                                  (put-long! b 0 n)
                                  (write-block! mbf b)
                                  (assoc m (get-id b) n)))
                              {} (range nr-blocks (* 2 nr-blocks)))]

        ;; check that all 200,000 numbers were stored as expected
        (doseq [[i r] (concat rand-mem rand-mem2)]
          (let [b (get-block mbf i)]
            (is (= r (get-long b 0)))))

        ;; rewind, to reuse the second 100,000 blocks
        (let [mbf (rewind! mbf)
              rand-mem3 (reduce (fn [m i]
                                  (let [n (long (rand nr-blocks))
                                        b (allocate-block! mbf)]
                                    (put-long! b 0 n)
                                    (write-block! mbf b)
                                    (assoc m (get-id b) n)))
                                {} (range nr-blocks (* 2 nr-blocks)))]

          ;; check that both first 100,000 is still right
          ;; and the second 100,000 contains the new data
          (doseq [[i r] (concat rand-mem rand-mem3)]
            (let [b (get-block mbf i)]
              (is (= r (get-long b 0)))))

          ;; drop the second 100,000 again
          (let [mbf (rewind! mbf)]
            ;; truncate the data after the first 100,000
            (close mbf)
            #?(:clj
               ;; file ManagedBlockFile, did the file get truncated to the first 100,000?
               (when (:block-file @(:state mbf))
                 (is (= (* nr-blocks Long/BYTES) (.length filename))))))))

      (finally
        (cleanup)
        (remove-file filename)))))
