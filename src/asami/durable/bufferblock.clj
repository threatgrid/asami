(ns ^{:doc "Abstraction for blocks of raw data, keyed by ID. IDs represent the offset of the block."
      :author "Paula Gearon"}
  asami.durable.block.bufferblock
  (:require [asami.durable.block.block-api :refer Block])
  (:import [java.nio ByteBuffer IntBuffer LongBuffer]))

;; An implementation of Block that can have multiple readers,
;; but only a single writing thread
(defrecord BufferBlock
    [^ByteBuffer bb ^IntBuffer ib ^LongBuffer lb ^ByteBuffer ro
     id size byte-offset int-offset long-offset]

    Block
    (get-id [this] id)
    
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
      b)

    (put-int! [this offset v]
      (.put ib (+ int-offset offset) v)
      b)

    (put-long! [this offset v]
      (.put lb (+ long-offset offset) v)
      b)

    ;; a single writer allows for position/put

    (put-bytes! [this offset ^bytes the-bytes]
      (doto bb (.position (+ byte-offset offset)) (.put the-bytes))
      b)

    (put-ints! [this offset ^ints the-ints]
      (doto ib (.position (+ int-offset offset)) (.put the-ints))
      b)

    (put-longs! [this offset ^longs the-longs]
      (doto lb (.position (+ long-offset offset)) (.put the-longs))
      b)

    (put-block!
      ([this offset ^BufferBlock src]
       (put-block! b offset src 0 (:size src)))
      ([this offset ^BufferBlock src src-offset length]
       (doto bb
         (.position (+ byte-offset offset))
         (.put (get-source-buffer src src-offset length)))
       b))

    (copy-over!
      [dest ^BufferBlock src offset]
      (put-block! dest 0 src offset size)))


(defn- new-block
  "Internal implementation for creating a BufferBlock using a set of buffers.
   If lb is nil, then ib must also be nil"
  [bb ib lb ro id size byte-offset]
  (assert (or (and ib lb) (not (or ib lb))) "int and long buffers must be provided or excluded together")
  (let [ib (or ib (-> bb .rewind .asIntBuffer))
        lb (or lb (-> bb .asLongBuffer))
        ro (or ro (.asReadOnlyBuffer bb))
        int-offset (bit-shift-right byte-offset 2)
        long-offset (bit-shift-right byte-offset 3)]
    (->BufferBlock bb ib lb ro id size byte-offset int-offset long-offset)))

(defn ^BufferBlock create-block
  "Wraps provided buffers as a block"
  ([id size byte-offset byte-buffer ro-byte-buffer int-buffer long-buffer]
   (new-block byte-buffer int-buffer long-buffer ro-byte-buffer id size byte-offset))
  ([id size byte-offset byte-buffer]
   (new-block byte-buffer nil nil nil id size byte-offset)))


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
