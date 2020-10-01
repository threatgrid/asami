(ns ^{:doc "Tests flat store functionality, saving and retrieving data"
      :author "Paula Gearon"}
    asami.durable.flat-test
  (:require [asami.durable.flat :refer [write-object! get-object force!]]
            #?(:clj [asami.durable.flat-file :as ff])
            #?(:clj [clojure.java.io :as io])
            [clojure.test :refer [deftest is]])
  #?(:clj (:import [java.net URI])))

(def store-name "test-fstore")

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
    #?(:clj (is (not (.exists (io/file store-name ff/file-name)))))
    (with-open [store (ff/flat-store store-name)]
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

    (with-open [store (ff/flat-store store-name)]
      (doseq [[n id] @fmapped]
        (is (= (nth data n) (get-object store id))))
      (doseq [[n id] (shuffle (seq @fmapped))]
        (is (= (nth data n) (get-object store id))))))
  #?(:clj
     (let [f (io/file store-name ff/file-name)
           d (io/file store-name)]
       (is (.delete f))
       (is (.delete d)))))
