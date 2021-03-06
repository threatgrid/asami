(ns ^{:doc "Common block testing code"
      :author "Paula Gearon"}
    asami.durable.block.test-util
  (:require [asami.durable.block.block-api :refer [put-byte! put-bytes! get-byte get-bytes]]
            #?(:cljs [goog.crypt :as crypt]))
  #?(:clj (:import [java.nio.charset Charset])))


(def str0 "String in block 0.")

(def str1 "String in block 1.")

(def str2 "String in block 2.")

(def str3 "String in block 3.")


#?(:clj (def utf8 (Charset/forName "UTF-8")))

(defn put-string! [b s]
  (let [^bytes bytes #?(:clj (.getBytes s utf8)
                        :cljs (crypt/stringToUtf8ByteArray s))
        len (count bytes)]
    (put-byte! b 0 len)
    (put-bytes! b 1 len bytes)))

(defn get-string [b]
  (let [l (get-byte b 0)
        d (get-bytes b 1 l)]
    #?(:clj (String. d utf8)
       :cljs (crypt/utf8ByteArrayToString d))))

