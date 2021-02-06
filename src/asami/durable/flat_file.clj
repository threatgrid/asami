(ns ^{:doc "Manages a memory-mapped file that holds write once data"
      :author "Paula Gearon"}
    asami.durable.flat-file
  (:require [clojure.java.io :as io]
            [asami.durable.common :refer [Paged refresh! read-byte read-bytes-into read-long
                                          FlatStore write-object! get-object force!
                                          TxStore Closeable Forceable get-tx tx-count long-size
                                          FlatRecords]]
            [asami.durable.encoder :as encoder]
            [asami.durable.decoder :as decoder])
  (:import [java.io RandomAccessFile]
           [java.nio ByteBuffer]
           [java.nio.channels FileChannel FileChannel$MapMode]))

(def read-only FileChannel$MapMode/READ_ONLY)

(def ^:const default-region-size "Default region of 1GB" 0x40000000)

(defprotocol Clearable
  (clear! [this] "Clears out any resources which may be held"))

(defn read-setup
  [{:keys [regions region-size] :as paged-file} offset byte-count]
  (let [region-nr (int (/ offset region-size))
        region-offset (mod offset region-size)]
    ;; the requested data is not currently mapped, so refresh
    (when (>= region-nr (count @regions))
      (refresh! paged-file))
    (when (>= region-nr (count @regions))
      (throw (ex-info "Accessing data beyond the end of file"
                      {:max (count @regions) :region region-nr :offset offset})))
    (let [region (nth @regions region-nr)
          region-size (.capacity region)
          end (+ byte-count region-offset)
          [region region-size] (if (and (= (inc region-nr) (count @regions))
                                        (>= end region-size))
                                 (do
                                   (refresh! paged-file)
                                   (let [^ByteBuffer r (nth @regions region-nr)]
                                     [r (.capacity r)]))
                                 [region region-size])]
      (when (> end region-size)
        (when (= region-nr (dec (count @regions)))
          (refresh! paged-file))
        (when (>= region-nr (dec (count @regions)))
          (throw (ex-info "Accessing trailing data beyond the end of file"
                          {:region-size region-size :region-offset region-offset}))))
      [region region-offset])))

;; These functions do update the PagedFile state, but only to expand the mapped region.
(defrecord PagedFile [^RandomAccessFile f regions region-size]
  Paged
  (refresh! [this]
    (letfn [(remap [mappings]
              (let [existing (if-let [tail (last mappings)]
                               (if (< (.capacity tail) region-size)
                                 (butlast mappings)
                                 mappings))
                    unmapped-offset (* region-size (count existing))
                    ^FileChannel fchannel (.getChannel f)
                    _ (.force fchannel true)
                    flength (.length f)
                    new-maps (map
                              (fn [offset]
                                (.map fchannel read-only offset (min region-size (- flength offset))))
                              (range unmapped-offset flength region-size))]
                (into [] (concat existing new-maps))))]
      (swap! regions remap)))

  (read-byte [this offset]
    ;; finds a byte in a region
    (let [[region region-offset] (read-setup this offset 1)]
      (.get ^ByteBuffer region region-offset)))

  (read-short [this offset]
    ;; when the 2 bytes occur in the same region, read a short
    ;; if the bytes straddle regions, then read both bytes and combine into a short
    (let [[region region-offset] (read-setup this offset 2)]
      (if (= region-offset (dec region-size))
        (short (bit-or (bit-shift-left (.get ^ByteBuffer region region-offset) 8)
                       (bit-and 0xFF (read-byte this (inc offset)))))
        (.getShort ^ByteBuffer region region-offset))))

  (read-long [this offset]
    ;; Unlike other types, a long is required to exist entirely in a region
    (let [[region region-offset] (read-setup this offset long-size)]
      (.getLong ^ByteBuffer region region-offset)))

  (read-bytes [this offset len]
    (read-bytes-into this offset (byte-array len)))

  (read-bytes-into [this offset bytes]
    ;; when the bytes occur entirely in a region, then return a slice of the region
    ;; if the bytes straddle 2 regions, create a new buffer, and copy the bytes from both regions into it
    (let [region-nr (int (/ offset region-size))
          region-offset (mod offset region-size)
          array-len (count bytes)]
      ;; the requested data is not currently mapped, so refresh
      (when (>= region-nr (count @regions))
        (refresh! this))
      (when (> array-len region-size)
        (throw (ex-info "Data size beyond size limit"
                        {:requested array-len :limit region-size})))
      (when (>= region-nr (count @regions))
        (throw (ex-info "Accessing data beyond the end of file"
                        {:max (count @regions) :region region-nr :offset offset})))
      (letfn [(read-bytes [attempt]
                (let [region (nth @regions region-nr)
                      region-size (.capacity region)]
                  (if (>= region-offset region-size)
                    (if (< attempt 1)
                      (do
                        (refresh! this)
                        (recur 1))
                      (throw (ex-info "Accessing trailing data beyond the end of file"
                                      {:region-size region-size :region-offset region-offset})))
                    
                    ;; check if the requested data is all in the same region
                    (if (> (+ region-offset array-len) region-size)
                      (do ;; data straddles 2 regions
                        (when (>= (inc region-nr) (count @regions))
                          (throw (ex-info "Accessing data beyond the end of file"
                                          {:max (count @regions) :region region-nr :offset offset})))
                        (let [nregion (nth @regions (inc region-nr))
                              fslice-size (- region-size region-offset)
                              nslice-size (- array-len fslice-size)]
                          (if (> nslice-size (.capacity nregion))
                            (if (< attempt 1)
                              (do
                                (refresh! this)
                                (recur 1))
                              (throw (ex-info "Accessing data beyond the end of file"
                                              {:size nslice-size :limit (.capacity nregion)})))
                            (do
                              (doto (.asReadOnlyBuffer region)
                                (.position region-offset)
                                (.get bytes 0 fslice-size))
                              (doto (.asReadOnlyBuffer nregion)
                                (.get bytes fslice-size nslice-size))
                              bytes))))
                      (do
                        (doto (.asReadOnlyBuffer region)
                          (.position region-offset)
                          (.get bytes))
                        bytes)))))]
        (read-bytes 0))))
  Clearable
  (clear! [this] (reset! regions nil)))

(defn paged-file
  "Creates a paged file reader"
  ([f] (paged-file f default-region-size))
  ([f region-size]
   (let [p (->PagedFile f (atom nil) region-size)]
     (refresh! p)
     p)))

;; rfile: A file that will only be appended to
;; paged: A paged reader for the file
(defrecord FlatFile [^RandomAccessFile rfile paged]
  FlatStore
  (write-object!
    [this obj]
    (let [id (.getFilePointer rfile)
          [hdr data] (encoder/to-bytes obj)]
      (.write rfile hdr)
      (.write rfile data)
      id))
  (get-object
    [this id]
    (decoder/read-object paged id))

  Forceable
  (force! [this]
    (.force (.getChannel ^RandomAccessFile rfile) true))

  Closeable
  (close [this]
    (force! this)
    (clear! paged)
    (.close rfile)))


(defn tx-file-size
  [rfile tx-size]
  (let [fsize (.getFilePointer rfile)]
    (when-not (zero? (mod fsize tx-size))
      (throw (ex-info "Corrupted transaction file" {:file-size fsize :tx-size tx-size})))
    fsize))

(defrecord TxFile [^RandomAccessFile rfile paged tx-size]
  TxStore
  (append!
    [this {:keys [timestamp tx-data] :as tx}]
    (let [sz (.getFilePointer rfile)]
      (.writeLong ^RandomAccessFile rfile ^long timestamp)
      (doseq [t tx-data]
        (.writeLong ^RandomAccessFile rfile ^long t))
      (long (/ sz tx-size))))

  (get-tx
    [this id]
    (let [offset (* tx-size id)
          timestamp (read-long paged offset)
          tx-data (mapv #(read-long paged (+ (* long-size %) offset)) (range 1 (/ tx-size long-size)))]
      {:timestamp timestamp
       :tx-data tx-data}))

  (latest
    [this]
    (let [fsize (tx-file-size rfile tx-size)
          id (dec (long (/ fsize tx-size)))]
      (when (<= 0 id)
        (get-tx this id))))

  (tx-count
    [this]
    (long (/ (tx-file-size rfile tx-size) tx-size)))

  (find-tx
    [this timestamp]
    (loop [low 0 high (tx-count this)]
      (if (= (inc low) high)
        low
        (let [mid (long (/ (+ low high) 2))
              mts (read-long paged (* tx-size mid))
              c (compare mts timestamp)]
          (cond
            (zero? c) mid
            (> 0 c) (recur mid high)
            (< 0 c) (recur low mid))))))

  Forceable
  (force! [this]
    (.force (.getChannel rfile) true))

  Closeable
  (close [this]
    (force! this)
    (clear! paged)
    (.close rfile)))

(defrecord RecordsFile [^RandomAccessFile rfile paged record-size]
  FlatRecords
  (append!
    [this data]
    (let [sz (.getFilePointer rfile)]
      (doseq [t data]
        (.writeLong ^RandomAccessFile rfile ^long t))
      (long (/ sz record-size))))

  (get-object
    [this id]
    (let [offset (* record-size id)
          timestamp (read-long paged offset)]
      (mapv #(read-long paged (+ (* long-size %) offset))
            (range 1 (/ record-size long-size)))))

  (next-id
    [this]
    (long (/ (.getFilePointer rfile) record-size)))

  Forceable
  (force! [this]
    (.force (.getChannel rfile) true))

  Closeable
  (close [this]
    (force! this)
    (clear! paged)
    (.close rfile)))

(defn- file-store
  "Creates and initializes an append-only file and a paged reader."
  [name fname size]
  (let [d (io/file name)
        _ (.mkdirs d)
        f (io/file name fname)
        raf (RandomAccessFile. f "rw")
        file-length (.length raf)]
    (when-not (zero? file-length)
      (.seek raf file-length))
    [raf (paged-file raf size)]))

(defn flat-store
  "Creates a flat file store. This wraps an append-only file and a paged reader."
  [group-name name]
  (let [[raf paged] (file-store group-name name default-region-size)]
    (->FlatFile raf paged)))

(defn block-file
  [group-name name record-size]
  (let [region-size (* record-size (int (/ default-region-size record-size)))]
    (file-store group-name name region-size)))

(defn tx-store
  "Creates a transaction store. This wraps an append-only file and a paged reader."
  [group-name name payload-size]
  (let [tx-size (+ long-size payload-size)
        [raf paged] (block-file group-name name tx-size)]
    (->TxFile raf paged tx-size)))

(defn record-store
  "Creates a record store. This wraps an append-only file and a paged reader."
  [group-name name record-size]
  (let [[raf paged] (block-file group-name name record-size)]
    (->RecordsFile raf paged record-size)))

(defn store-exists?
  "Checks if the resources for a file have been created already"
  [group-name name]
  (let [d (io/file group-name)
        f (io/file group-name fname)]
    (and (.exists d) (.exists f))))
