(ns ^{:doc "Common functions for storage based tests"
      :author "Paula Gearon"}
    asami.durable.test-utils
    (:require [asami.durable.block.block-api :refer [Block]]
              #?(:clj
                 [asami.durable.block.file.util :as util])
              #?(:clj
                 [asami.durable.block.bufferblock :refer [create-block]]))
    #?(:clj (:import [java.io File]
                     [java.nio ByteBuffer])))

(defn get-filename
  "Returns the resource for creating a manager.
  For Java, this is a java.io.File. On JS this is a string."
  [s]
  #?(:clj (let [f (util/temp-file s)]
            (doto ^File f .delete))
     :cljs s))

#?(:cljs
   ;; Based on code by Joel Holdbrooks
   (defrecord BufferBlock [id ^js/DataView __dataView __position __len]
     Block
     (get-id [this] id)

     (get-byte [this offset]
       (.getInt8 __dataView (+ __position offset)))

     (get-int [this offset]
       (.getInt32 __dataView (+ __position (* offset int-size))))

     (get-long [this offset]
       (.getBigInt64 __dataView (+ __position (* offset long-size))))

     (get-bytes [this offset len]
       (let [arr (js/Array len)
             offset_ (+ __position offset)]
         (doseq [n (range (min len __len))]
           (aset arr n (.getInt8 __dataView (+ offset_ n))))
         arr))

     (get-ints [this offset len]
       (let [arr (js/Array len)]
         (doseq [n (min (range len) (/ __len int-size))]
           (aset arr n (.getInt32 __dataView (+ __position (* (+ offset n) int-size)))))
         arr))

     (get-longs [this offset len]
       (let [arr (js/Array len)]
         (doseq [n (range (min len (/ __len long-size)))]
           (aset arr n (.getBigInt64 __dataView (+ __position (* (+ offset n) long-size)))))
         arr))

     (put-byte! [this offset value]
       (.setInt8 __dataView (+ __position offset) value)
       this)

     (put-int! [this offset value]
       (.setInt32 __dataView (+ __position (* offset int-size)) value)
       this)

     (put-long! [this offset value]
       (.setBigInt64 __dataView (+ __position (* offset long-size)) value)
       this)

     (put-bytes! [this offset len values]
       (doseq [n (range (min len __size))]
         (.setInt8 __dataView (+ __position offset n) (aget values n)))
       this)

     (put-ints! [this offset len values]
       (doseq [n (range (min len (/ __size int-size)))]
         (.setInt32 __dataView (+ __position (* (+ offset n) int-size)) (aget values n)))
       this)

     (put-longs! [this offset len values]
       (doseq [n (range (min len (/ __size long-size)))]
         (.setInt8 __dataView (+ __position (* (+ offset n) long-size)) (aget values n)))
       this)

     (put-block!
       [this offset src src-offset length]
       (let [offset_ (+ __position offset)
             sdv (:__dataView src)]
         (doseq [n (range (min length (- __size offset) (- (:__size src) src-offset)))]
           (.setInt8 __dataView
                     (+ offset_ n)
                     (.getInt8 sdv (+ src-offset n))))))

     (put-block!
       [this offset src]
       (put-block! this offset src 0 (:__size src)))

     (copy-over! [this src src-offset]
       (put-block! this 0 src src-offset (- (:__size src) src-offset)))))


(defn new-block
  ([len] (new-block len 0))
  ([len id]
   #?(:clj
      (let [ba (byte-array len)
            bb (ByteBuffer/wrap ba)
            ib (.asIntBuffer bb)
            lb (.asLongBuffer bb)]
        (create-block id len 0 bb bb ib lb))

      :cljs
      (->BufferBlock (js/DataView. (js/ArrayBuffer. len)) id len))))
