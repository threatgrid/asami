(ns ^{:doc "Common functions for storage based tests"
      :author "Paula Gearon"}
    asami.durable.test-utils
    (:require [asami.durable.block.block-api :refer [Block put-block!]]
              [asami.durable.common :refer [long-size int-size]]
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
       (doseq [n (range (min len __len))]
         (.setInt8 __dataView (+ __position offset n) (aget values n)))
       this)

     (put-ints! [this offset len values]
       (doseq [n (range (min len (/ __len int-size)))]
         (.setInt32 __dataView (+ __position (* (+ offset n) int-size)) (aget values n)))
       this)

     (put-longs! [this offset len values]
       (doseq [n (range (min len (/ __len long-size)))]
         (.setInt8 __dataView (+ __position (* (+ offset n) long-size)) (aget values n)))
       this)

     (put-block!
       [this offset src src-offset length]
       (let [offset_ (+ __position offset)
             sdv (:__dataView src)]
         (doseq [n (range (min length (- __len offset) (- (:__len src) src-offset)))]
           (.setInt8 __dataView
                     (+ offset_ n)
                     (.getInt8 sdv (+ src-offset n))))))

     (put-block!
       [this offset src]
       (put-block! this offset src 0 (:__len src)))

     (copy-over! [this src src-offset]
       (put-block! this 0 src src-offset (- (:__len src) src-offset)))))


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
      (->BufferBlock id (js/DataView. (js/ArrayBuffer. len)) 0 len))))

(defmacro assert-args
  [& pairs]
  `(do (when-not ~(first pairs)
         (throw (IllegalArgumentException.
                  (str (first ~'&form) " requires " ~(second pairs) " in " ~'*ns* ":" (:line (meta ~'&form))))))
     ~(let [more (nnext pairs)]
        (when more
          (list* `assert-args more)))))

(defmacro with-cleanup
  "bindings => [name init ...]

  Evaluates body in a try expression with names bound to the values
  of the inits, and a finally clause that calls (close name) on each
  name in reverse order, followed by delete!."
  {:added "1.0"}
  [bindings & body]
  (assert-args
     (vector? bindings) "a vector for its binding"
     (even? (count bindings)) "an even number of forms in binding vector")
  (cond
    (= (count bindings) 0) `(do ~@body)
    (symbol? (bindings 0)) `(let ~(subvec bindings 0 2)
                              (try
                                (with-cleanup ~(subvec bindings 2) ~@body)
                                (finally
                                  (asami.durable.common/close ~(bindings 0))
                                  (asami.durable.common/delete! ~(bindings 0)))))
    :else (throw (ex-info "with- only allows Symbols in bindings" {:bindings bindings}))))
