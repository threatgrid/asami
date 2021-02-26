(ns ^{:doc "Tests the coder/decoder code for accessing index blocks"
      :author "Paula Gearon"}
    asami.durable.idx-codec-test
    (:require [clojure.test #?(:clj :refer :cljs :refer-macros) [deftest is]]
              [asami.durable.test-utils :refer [new-block]]
              [asami.durable.encoder :refer [to-bytes]]
              [asami.durable.decoder :refer [long-bytes-compare]]
              [asami.durable.common :as common :refer [int-size]]
              [asami.durable.pool :as pool :refer [index-writer]]
              [asami.durable.block.block-api :refer [get-bytes]]))


#?(:cljs
   (defn byte-array
     [len] (js/ArrayBuffer. len)))

(defn check-data
  ([obj] (check-data obj obj))
  ([obj other]
   (check-data obj
               other
               (cond
                 (string? obj) 2
                 (keyword? obj) 10
                 (uri? obj) 3
                 :default 0)))
  ([obj other typ]
   (let [node (new-block pool/tree-node-size)
         [header body :as data] (to-bytes obj)
         [oheader obody :as odata] (to-bytes other)]
     (index-writer node [data 0])
     (if (identical? obj other)
       (is (= 0 (long-bytes-compare typ oheader obody other
                                    (get-bytes node pool/data-offset pool/payload-len)))
           (str "comparing: '" obj "' / '" other "'"))
       (is (not= 0 (long-bytes-compare typ oheader obody other
                                       (get-bytes node pool/data-offset pool/payload-len)))
           (str "comparing: '" obj "' / '" other "'"))))))

(deftest index-codec
  (check-data "of")
  (check-data "of" "if")
  (check-data "of" "off")
  (check-data "one thousand")
  (check-data "one thousand" "one")
  (check-data "Really, really, really, really long data. No, really. It's that long. And longer."))
