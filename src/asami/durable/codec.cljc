(ns ^{:doc "Common encoding and decoding values"
      :author "Paula Gearon"}
    asami.durable.codec
    #?(:clj (:import [java.nio.charset Charset])))


(def ^:const byte-mask 0xFF)
(def ^:const data-mask 0x0FFFFFFFFFFFFFFF)
(def ^:const sbytes-shift 48)
(def ^:const len-nybble-shift 56)

(def utf8 #?(:clj (Charset/forName "UTF-8")
             :cljs "UTF-8"))


;; Encapsualted IDs are IDs containing all of the information without requiring additional storage
;; The data type is contained in the top 4 bits. The remaining 60 bit hold the data:
;; Top 4 bits:
;; 1 0 0 0: long
;; 1 1 0 0: Date
;; 1 0 1 0: Instant
;; 1 1 1 0: Short String
;; 1 0 0 1: Short Keyword
;; 1 1 0 1: Internal Node - asami.graph/InternalNode

(def ^:const long-type-code 0x8)
(def ^:const date-type-code 0xC)
(def ^:const inst-type-code 0xA)
(def ^:const sstr-type-code 0xE)
(def ^:const skey-type-code 0x9)
(def ^:const node-type-code 0xD)

(def ^:const long-type-mask (bit-shift-left long-type-code 60))
(def ^:const date-type-mask (bit-shift-left date-type-code 60))
(def ^:const inst-type-mask (bit-shift-left inst-type-code 60))
(def ^:const sstr-type-mask (bit-shift-left sstr-type-code 60))
(def ^:const skey-type-mask (bit-shift-left skey-type-code 60))
(def ^:const node-type-mask (bit-shift-left node-type-code 60))

