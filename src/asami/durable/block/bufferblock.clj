(ns ^{:doc "Abstraction for blocks of raw data, keyed by ID. IDs represent the offset of the block."
      :author "Paula Gearon"}
  asami.durable.block.bufferblock
  (:require [asami.durable.block.block-api :refer [Block put-block!]])
  (:import [java.nio ByteBuffer IntBuffer LongBuffer]))

;; An implementation of Block that can have multiple readers,
;; but only a single writing thread
(defrecord BufferBlock
    [^ByteBuffer bb ^IntBuffer ib ^LongBuffer lb
     ^ByteBuffer ro
     byte-offset int-offset long-offset]

    Block
    (get-byte [this offset]
      (.get bb (+ byte-offset offset)))

    (get-int [this offset]
      (.get ib (+ int-offset offset)))

    (get-long [this offset]
      (.get lb (+ long-offset offset)))

    (get-bytes [this offset len]
      (let [^ByteBuffer tbb (.duplicate bb)
            start (+ byte-offset offset)
            arr (byte-array len)]
        (doto tbb
          (.position start)
          (.limit (+ start len))
          (.get arr))
        arr))

    (get-ints [this offset len]
      (let [^IntBuffer tib (.duplicate ib)
            start (+ int-offset offset)
            arr (int-array len)]
        (doto tib
          (.position start)
          (.limit (+ start len))
          (.get arr))
        arr))

    (get-longs [this offset len]
      (let [^LongBuffer tlb (.duplicate lb)
            start (+ long-offset offset)
            arr (long-array len)]
        (doto tlb
          (.position start)
          (.limit (+ start len))
          (.get arr))
        arr))

    (put-byte! [this offset v]
      (.put bb (+ byte-offset offset) v)
      this)

    (put-int! [this offset v]
      (.put ib (+ int-offset offset) v)
      this)

    (put-long! [this offset v]
      (.put lb (+ long-offset offset) v)
      this)

    ;; a single writer allows for position/put

    (put-bytes! [this offset len the-bytes]
      (doto bb (.position (+ byte-offset offset)) (.put the-bytes 0 len))
      this)

    (put-ints! [this offset len the-ints]
      (doto ib (.position (+ int-offset offset)) (.put the-ints 0 len))
      this)

    (put-longs! [this offset len the-longs]
      (doto lb (.position (+ long-offset offset)) (.put the-longs 0 len))
      this)

    (put-block!
      [this offset src src-offset length]
      (let [sbb (:bb src)]
        (doto sbb
          (.position src-offset)
          (.limit (+ src-offset length)))
        (doto bb
          (.position (+ byte-offset offset))
          (.put sbb src-offset length)))
      this)

    (put-block!
      [this offset src]
      (put-block! this offset src 0 (.capacity ^ByteBuffer (:bb src))))

    (copy-over!
      [dest src offset]
      (put-block! dest 0 src offset (.capacity bb))))


(defn- new-block
  "Internal implementation for creating a BufferBlock using a set of buffers.
   If lb is nil, then ib must also be nil"
  [bb ib lb ro byte-offset]
  (assert (or (and ib lb) (not (or ib lb))) "int and long buffers must be provided or excluded together")
  (let [ib (or ib (-> bb .rewind .asIntBuffer))
        lb (or lb (-> bb .asLongBuffer))
        ro (or ro (.asReadOnlyBuffer bb))
        int-offset (bit-shift-right byte-offset 2)
        long-offset (bit-shift-right byte-offset 3)]
    (->BufferBlock bb ib lb ro byte-offset int-offset long-offset)))

(defn ^BufferBlock create-block
  "Wraps provided buffers as a block"
  ([size byte-offset byte-buffer ro-byte-buffer int-buffer long-buffer]
   (new-block byte-buffer int-buffer long-buffer ro-byte-buffer size byte-offset))
  ([size byte-offset byte-buffer]
   (new-block byte-buffer nil nil nil size byte-offset)))


;; The following functions are ByteBuffer specfic,
;; and are not available on the general Block API

(defn ^ByteBuffer get-source-buffer
  "Returns a read-only ByteBuffer for the block"
  ([^BufferBlock {:keys [ro bb]}] (or ro (.asReadOnlyBuffer bb)))
  ([^BufferBlock b offset length]
   (let [start (+ (:byte-offset b) offset)]
     (doto (get-source-buffer b)
           (.limit (+ start length))
           (.position start)))))


(defn ^ByteBuffer copy-to-buffer! [^BufferBlock b ^ByteBuffer buffer offset]
  "Copies the contents of a ByteBuffer into the block."
  (let [pos (+ (:byte-offset b) offset)]
    (.put buffer (doto (.asReadOnlyBuffer (:bb b))
                       (.position pos)
                       (.limit (+ pos (.remaining buffer)))))
    buffer))

(defn ^ByteBuffer slice [^BufferBlock b offset size]
  "Returns a portion of a block as a ByteBuffer"
  (let [pos (+ (:byte-offset b) offset)]
    (.slice (doto (.asReadOnlyBuffer (:bb b))
                  (.position pos)
                  (.limit (+ pos size))))))

(defn ^BufferBlock put-buffer! [^BufferBlock b offset ^ByteBuffer buffer]
  (doto (:bb b) (.position (+ (:byte-offset b) offset)) (.put buffer))
  b)
