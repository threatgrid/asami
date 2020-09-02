(ns ^{:doc "Manages a memory-mapped file that holds write once data"
      :author "Paula Gearon"}
    asami.durable.flat-file
  (:require [clojure.java.io :as io]
            [asami.durable.pages :refer [Paged refresh! read-byte read-bytes-into]]
            [asami.durable.flat :refer [FlatStore write-object! get-object force!]]
            [asami.durable.encoder :as encoder]
            [asami.durable.decoder :as decoder])
  (:import [java.io RandomAccessFile]
           [java.nio.channels FileChannel FileChannel$MapMode]))

(def read-only FileChannel$MapMode/READ_ONLY)

(def ^:const default-region-size "Default region of 1GB" 0x40000000)

(def file-name "raw.dat")

(defprotocol Clearable
  (clear! [this] "Clears out any resources which may be held"))

;; These functions do update the PagedFile state, but only to expand the mapped region.
(defrecord PagedFile [^RandomAccessFile f regions region-size]
  Paged
  (refresh! [this]
    (letfn [(remap [mappings]
              (let [existing (or (if-let [tail (last mappings)]
                                   (if (< (.capacity tail) region-size)
                                     (butlast mappings)))
                                 mappings)
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
    (let [region-nr (int (/ offset region-size))
          region-offset (mod offset region-size)
          region-count (count @regions)
          region (if (>= region-nr region-count) ;; region not mapped
                   (do
                     (refresh! this)
                     (when (>= region-nr (count @regions))
                       (throw (ex-info "Accessing data beyond the end of file"
                                       {:max (count @regions) :region region-nr :offset offset})))
                     (nth @regions region-nr))
                   (let [dregion-count (dec region-count)]
                     (if (= region-nr dregion-count) ;; referencing final region
                       (let [last-region (nth @regions dregion-count)]
                         (if (>= region-offset (.capacity last-region))
                           (do
                             (refresh! this)
                             (let [refreshed-region (nth @regions dregion-count)]
                               (when (>= region-offset (.capacity refreshed-region))
                                 (throw (ex-info "Accessing data beyond the end of mapped file"
                                                 {:region-offset region-offset
                                                  :region region-nr
                                                  :offset offset})))
                               refreshed-region))
                           last-region))
                       (nth @regions region-nr))))]
      (.get region region-offset)))

  (read-short [this offset]
    ;; when the 2 bytes occur in the same region, read a short
    ;; if the bytes straddle regions, then read both byts and combine into a short
    (let [region-nr (int (/ offset region-size))
          region-offset (mod offset region-size)]
      ;; the requested data is not currently mapped, so refresh
      (when (>= region-nr (count @regions))
        (refresh! this))
      (when (>= region-nr (count @regions))
        (throw (ex-info "Accessing data beyond the end of file"
                        {:max (count @regions) :region region-nr :offset offset})))
      (let [region (nth @regions region-nr)
            region-size (.capacity region)]
        (when (>= region-offset region-size)
          (throw (ex-info "Accessing trailing data beyond the end of file"
                          {:region-size region-size :region-offset region-offset})))
        (if (= region-offset (dec region-size))
          (short (bit-or (bit-shift-left (.get region region-offset) 8)
                         (bit-and 0xFF (read-byte this (inc offset)))))
          (.getShort region region-offset)))))

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
      (let [region (nth @regions region-nr)
            region-size (.capacity region)]
        (when (>= region-offset region-size)
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
              (when (> nslice-size (.capacity nregion))
                (throw (ex-info "Accessing data beyond the end of file"
                                {:size nslice-size :limit (.capacity nregion)})))
              (doto (.asReadOnlyBuffer region)
                (.position region-offset)
                (.get bytes 0 fslice-size))
              (doto (.asReadOnlyBuffer nregion)
                (.get bytes fslice-size nslice-size))))
          (doto (.asReadOnlyBuffer region)
            (.position region-offset)
            (.get bytes)))
        bytes)))
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
  (force! [this]
    (.force (.getChannel rfile) true))
  (close [this]
    (clear! paged)
    (.close rfile)))

(defn flat-store
  "Creates a flat file store. This wraps an append-only file and a paged reader."
  [name]
  (let [d (io/file name)
        _ (.mkdirs d)
        f (io/file name file-name)
        raf (RandomAccessFile. f "rw")
        file-length (.length raf)]
    (when-not (zero? file-length)
      (.seek raf file-length))
    (->FlatFile raf (paged-file raf))))
