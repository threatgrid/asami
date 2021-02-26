(ns ^{:doc "Tests flat store functionality, saving and retrieving data"
      :author "Paula Gearon"}
    asami.durable.flat-test
    (:require [asami.durable.common :refer [write-object! get-object force! append-tx!
                                            long-size get-tx latest tx-count find-tx close delete!
                                            append! get-record next-id]]
              [asami.durable.test-utils :as util :include-macros true]
              #?(:clj [asami.durable.flat-file :as ff])
              #?(:clj [clojure.java.io :as io])
              [clojure.test #?(:clj :refer :cljs :refer-macros) [deftest is]])
    #?(:clj (:import [java.net URI])))

(def store-name "test-fstore")

(def flat-name "raw.bin")
(def tx-name "tx.bin")

(defn uri [s] #?(:clj (URI. s) :cljs (goog/Uri. s)))

(def data
  ["Hello"
   "Goodbye"
   :the
   :quick
   (uri "http://brown.com/fox")
   "jumps over the lazy dog."
   1812
   "It is a truth universally acknowledged, that a single man in possession of a good fortune, must be in want of a wife.\n\nHowever little known the feelings or views of such a man may be on his first entering a neighbourhood, this truth is so well fixed in the minds of the surrounding families, that he is considered the rightful property of some one or other of their daughters.\n\n“My dear Mr. Bennet,” said his lady to him one day, “have you heard that Netherfield Park is let at last?”\n\nMr. Bennet replied that he had not.\n\n“But it is,” returned she; “for Mrs. Long has just been here, and she told me all about it.”\n\nMr. Bennet made no answer.\n\n“Do you not want to know who has taken it?” cried his wife impatiently.\n\n“You want to tell me, and I have no objection to hearing it.”\n\nThis was invitation enough."])

;; Clojure specific parts are for clearing out the temporary files
(deftest test-store
  (let [fmapped (volatile! nil)]
    #?(:clj (is (not (.exists (io/file store-name flat-name)))))
    (with-open [store (ff/flat-store store-name flat-name)]
      (vreset! fmapped (reduce-kv (fn [m k v]
                                    (let [id (write-object! store v)]
                                      (assoc m k id)))
                                  {} data))
      (is (= (count data) (count @fmapped)))
      (doseq [[n id] @fmapped]
        (is (= (nth data n) (get-object store id))))
      (doseq [[n id] (shuffle (seq @fmapped))]
        (is (= (nth data n) (get-object store id))))
      (force! store))

    (util/with-cleanup [store (ff/flat-store store-name flat-name)]
      (doseq [[n id] @fmapped]
        (is (= (nth data n) (get-object store id))))
      (doseq [[n id] (shuffle (seq @fmapped))]
        (is (= (nth data n) (get-object store id))))))
  #?(:clj
     (let [d (io/file store-name)]
       (.delete d))))


(deftest test-tx-store
  #?(:clj (let [d (io/file store-name)
                f (io/file store-name tx-name)]
            (.mkdir d)
            (when (.exists f) (.delete f))))

  (let [store (ff/tx-store store-name tx-name long-size)]
    (is (nil? (latest store)))
    (doseq [t (range 0 10 2)]
      (append-tx! store {:timestamp t :tx-data [(* t t)]}))
    (is (= 5 (tx-count store)))
    (is (= {:timestamp 8 :tx-data [64]} (latest store)))
    (is (= [16] (:tx-data (get-tx store 2))))
    (is (= [36] (:tx-data (get-tx store 3))))
    (is (= 6 (:timestamp (get-tx store 3))))
    (doseq [t (range 10 20 2)]
      (append-tx! store {:timestamp t :tx-data [(* t t)]}))
    (is (= 10 (tx-count store)))
    (is (= 6 (:timestamp (get-tx store 3))))
    (is (= {:timestamp 18 :tx-data [324]} (latest store)))
    (doseq [n (range 10) :let [t (* 2 n) r (get-tx store n)]]
      (is (= {:timestamp t :tx-data [(* t t)]} r)))
    (close store))

  (let [store (ff/tx-store store-name tx-name long-size)]
    (is (= 10 (tx-count store)))
    (is (= {:timestamp 18 :tx-data [324]} (latest store)))
    (doseq [n (range 10) :let [t (* 2 n) r (get-tx store n)]]
      (is (= {:timestamp t :tx-data [(* t t)]} r)))
    (doseq [t (range 20 30 2)]
      (append-tx! store {:timestamp t :tx-data [(* t t)]}))
    (is (= {:timestamp 28 :tx-data [784]} (latest store)))
    (doseq [n (range 15) :let [t (* 2 n) r (get-tx store n)]]
      (is (= {:timestamp t :tx-data [(* t t)]} r)))

    (is (= 4 (find-tx store 8)))
    (is (= 5 (find-tx store 11)))
    (is (= 0 (find-tx store -1)))
    (is (= 14 (find-tx store 50)))

    (close store)
    (delete! store))

  #?(:clj (let [d (io/file store-name)]
            (.delete d))))

(deftest test-record-store
  (util/with-cleanup [records #?(:clj (ff/record-store "records" "rec.dat" (* 4 8)))]
    (is (= 0 (next-id records)))
    (doseq [x (range 1000) :let [xx (* x x)]]
      (append! records (vec (range xx (+ xx 4)))))
    (doseq [x (map #(mod (* 569 %) 1000) (range 1000)) :let [xx (* x x)]]
      (is (= (vec (range xx (+ 4 xx))) (get-record records x))))
    (is (= 1000 (next-id records)))))
