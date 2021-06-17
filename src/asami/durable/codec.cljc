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
;; 1 0 1 1: boolean - This leaves a 58 bit space for something else

(def ^:const long-type-code 0x8)
(def ^:const date-type-code 0xC)
(def ^:const inst-type-code 0xA)
(def ^:const sstr-type-code 0xE)
(def ^:const skey-type-code 0x9)
(def ^:const node-type-code 0xD)
(def ^:const bool-type-code 0xB)

(def ^:const long-type-mask (bit-shift-left long-type-code 60))
(def ^:const date-type-mask (bit-shift-left date-type-code 60))
(def ^:const inst-type-mask (bit-shift-left inst-type-code 60))
(def ^:const sstr-type-mask (bit-shift-left sstr-type-code 60))
(def ^:const skey-type-mask (bit-shift-left skey-type-code 60))
(def ^:const node-type-mask (bit-shift-left node-type-code 60))
(def ^:const bool-type-mask (bit-shift-left bool-type-code 60))


(def ^:const boolean-false-bits bool-type-mask)
(def ^:const boolean-true-bits (bit-or bool-type-mask (bit-shift-left 0x8 56)))

;; Header/Body description
;; Header tries to use as many bits for length data as possible. This cuts into the bit available for type data.
;; Byte 0
;; 0xxxxxxx  String type, length of up to 127.
;; 10xxxxxx  URI type, length of up to 64
;; 1100xxxx  Keyword type, length of up to 16
;;           For these 3 types, all remaining bytes are the data body.
;; 1101xxxx  Long value. xxxx encodes the number of bytes
;; 111ytttt  Data is of type described in tttt.
;;           Length is run-length encoded as follows:
;; When y=0
;; Byte 1
;; xxxxxxxx  The length of the data, 0-255
;;
;; When y=1
;; Length is run-length encoded
;; Bytes 1-2
;; 0xxxxxxx xxxxxxxx Length of the data, 256-32kB
;; 1xxxxxxx xxxxxxxx Indicates a 4-byte length 32kB-32GB
;; Bytes 3-4
;; zzzzzzzz zzzzzzzz When Byte 1 started with 1, then included with bytes 1-2 to provide 32GB length

;; NOTE: reconsidering using the y bit from byte 0 to indicate
;; that byte 1 is extra type information. This would allow for
;; short numerical types, types of URL that start with http://
;; and https:// etc.
