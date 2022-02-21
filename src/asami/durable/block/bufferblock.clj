(ns ^{:doc "Abstraction for blocks of raw data, keyed by ID. IDs represent the offset of the block."
      :author "Paula Gearon"}
  asami.durable.block.bufferblock
  (:require [asami.durable.block.block-api :refer [Block put-block!]])
  (:import [java.nio ByteBuffer IntBuffer LongBuffer]))

;; (set! *warn-on-reflection* true)

;; An implementation of Block that can have multiple readers,
;; but only a single writing thread
(defrecord BufferBlock
    [id
     ^ByteBuffer bb ^IntBuffer ib ^LongBuffer lb
     ^ByteBuffer ro
     size
     byte-offset int-offset long-offset]

    Block
    (get-id [this] id)

    (get-byte [this offset]
      (.get ^ByteBuffer bb (int (+ byte-offset offset))))

    (get-int [this offset]
      (.get ^IntBuffer ib (int (+ int-offset offset))))

    (get-long [this offset]
      (.get ^LongBuffer lb (int (+ long-offset offset))))

    (get-bytes [this offset len]
      (let [^ByteBuffer tbb (.duplicate bb)
            start (+ byte-offset offset)
            arr (byte-array len)]
        (doto tbb
          (.position (int start))
          (.limit (int (+ start len)))
          (.get arr))
        arr))

    (get-ints [this offset len]
      (let [^IntBuffer tib (.duplicate ib)
            start (+ int-offset offset)
            arr (int-array len)]
        (doto tib
          (.position (int start))
          (.limit (int (+ start len)))
          (.get arr))
        arr))

    (get-longs [this offset len]
      (let [^LongBuffer tlb (.duplicate lb)
            start (+ long-offset offset)
            arr (long-array len)]
        (doto tlb
          (.position (int start))
          (.limit (int (+ start len)))
          (.get arr))
        arr))

    (put-byte! [this offset v]
      (.put ^ByteBuffer bb (int (+ byte-offset offset)) (byte v))
      this)

    (put-int! [this offset v]
      (.put ^IntBuffer ib (int (+ int-offset offset)) (int v))
      this)

    (put-long! [this offset v]
      (.put ^LongBuffer lb (int (+ long-offset offset)) ^long v)
      this)

    ;; a single writer allows for position/put

    (put-bytes! [this offset len the-bytes]
      (doto ^ByteBuffer bb
        (.position (int (+ byte-offset offset)))
        (.put ^bytes the-bytes (int 0) (int len)))
      this)

    (put-ints! [this offset len the-ints]
      (doto ^IntBuffer ib
        (.position (int (+ int-offset offset)))
        (.put ^ints the-ints (int 0) (int len)))
      this)

    (put-longs! [this offset len the-longs]
      (doto ^LongBuffer lb
        (.position (int (+ long-offset offset)))
        (.put ^longs the-longs (int 0) (int len)))
      this)

    (put-block!
      [this offset {sbb :bb sbyte-offset :byte-offset :as src} src-offset length]
      (let [p (+ sbyte-offset src-offset)
            rsbb (.asReadOnlyBuffer ^ByteBuffer sbb)]
        (doto rsbb
          (.position (int p))
          (.limit (int (+ p length))))
        (doto ^ByteBuffer (.duplicate ^ByteBuffer bb)
          (.position (int (+ byte-offset offset)))
          (.put rsbb)))
      this)

    (put-block!
      [this offset src]
      (put-block! this offset src 0 (:size src)))

    (copy-over!
      [dest src offset]
      (put-block! dest 0 src offset size)))


(defn- new-block
  "Internal implementation for creating a BufferBlock using a set of buffers.
   If lb is nil, then ib must also be nil"
  [id ^ByteBuffer bb ib lb ro size byte-offset]
  (assert (or (and ib lb) (not (or ib lb))) "int and long buffers must be provided or excluded together")
  (let [ib (or ib (-> bb .rewind .asIntBuffer))
        lb (or lb (-> bb .asLongBuffer))
        ro (or ro (.asReadOnlyBuffer bb))
        int-offset (bit-shift-right byte-offset 2)
        long-offset (bit-shift-right byte-offset 3)]
    (->BufferBlock id bb ib lb ro size byte-offset int-offset long-offset)))

(defn ^BufferBlock create-block
  "Wraps provided buffers as a block"
  ([id size byte-offset byte-buffer ro-byte-buffer int-buffer long-buffer]
   (new-block id byte-buffer int-buffer long-buffer ro-byte-buffer size byte-offset))
  ([id size byte-offset byte-buffer]
   (new-block id byte-buffer nil nil nil size byte-offset)))


;; The following functions are ByteBuffer specfic,
;; and are not available on the general Block API

(defn ^ByteBuffer get-source-buffer
  "Returns a read-only ByteBuffer for the block"
  ([^BufferBlock {:keys [ro bb]}] (or ro (.asReadOnlyBuffer ^ByteBuffer bb)))
  ([^BufferBlock b offset length]
   (let [start (+ (:byte-offset b) offset)]
     (doto ^ByteBuffer (get-source-buffer b)
       (.limit (int (+ start length)))
       (.position (int start))))))


(defn ^ByteBuffer copy-to-buffer! [^BufferBlock b ^ByteBuffer buffer offset]
  "Copies the contents of a ByteBuffer into the block."
  (let [pos (+ (:byte-offset b) offset)]
    (.put buffer ^ByteBuffer (doto (.asReadOnlyBuffer ^ByteBuffer (:bb b))
                               (.position (int pos))
                               (.limit (int (+ pos (.remaining buffer))))))
    buffer))

(defn ^ByteBuffer slice [^BufferBlock b offset size]
  "Returns a portion of a block as a ByteBuffer"
  (let [pos (+ (:byte-offset b) offset)]
    (.slice (doto (.asReadOnlyBuffer ^ByteBuffer (:bb b))
              (.position (int pos))
              (.limit (int (+ pos size)))))))

(defn ^BufferBlock put-buffer! [^BufferBlock b offset ^ByteBuffer buffer]
  (doto ^ByteBuffer (:bb b) (.position (int (+ (:byte-offset b) offset))) (.put buffer))
  b)
