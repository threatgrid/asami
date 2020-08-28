(ns ^{:doc "Manages a memory-mapped file that holds write once data"
      :author "Paula Gearon"}
    asami.durable.flat
  (:import [java.io.RandomAccessFile]))

(defprotocol Paged
  (refresh! [this] "Refreshes the buffers")
  (read-byte [this offset] "Returns a byte from underlying pages")
  (read-short [this offset] "Returns a short from underlying pages. Offset in bytes.")
  (read-bytes [this offset bytes] "Fills a byte array with data from the paged object"))

;; These functions do update the PagedFile state, but only to expand the mapped region.
(defrecord PagedFile [f regions region-size]
  Paged
  (refresh! [this]
    ;; drop any partial final regions and remap to the end of the file
    )
  (read-byte [this offset]
    (let [region-nr (int (/ offset region-size))
          region-offset (mod offset region-size)]
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
        (.get region region-offset))))

  (read-short [this offset]
    (let [region-nr (int (/ offset region-size))
          region-offset (mod offset region-size)]
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
                         (read-byte this (inc offset))))
          (.getShort region (bit-shift-right region-offset 1))))))

  (read-bytes [this offset bytes]
    (let [region-nr (int (/ offset region-size))
          region-offset (mod offset region-size)
          array-size (count bytes)]
      (when (>= region-nr (count @regions))
        (refresh! this))
      (when (> array-size region-size)
        (throw (ex-info "Data size beyond size limit"
                        {:requested array-size :limit region-size})))
      (when (>= region-nr (count @regions))
        (throw (ex-info "Accessing data beyond the end of file"
                        {:max (count @regions) :region region-nr :offset offset})))
      (let [region (nth @regions region-nr)
            region-size (.capacity region)]
        (when (>= region-offset region-size)
          (throw (ex-info "Accessing trailing data beyond the end of file"
                          {:region-size region-size :region-offset region-offset})))
        (if (> (+ region-offset array-size) region-size)
          (do
            (when (>= (inc region-nr) (count @regions))
              (throw (ex-info "Accessing data beyond the end of file"
                              {:max (count @regions) :region region-nr :offset offset})))
            (let [nregion (nth @regions (inc region-nr))
                  fslice-size (- region-size region-offset)
                  nslice-size (- array-size fslice-size)]
              (when (> nslice-size (.capacity nregion))
                    (throw (ex-info "Accessing data beyond the end of file"
                              {:size nslice-size :limit (.capacity nregion)})))
              (doto (.asReadOnlyBuffer region)
                (.position region-offset)
                (.get bytes 0 fslice-size))
              (doto (.asReadOnlyBuffer nregion)
                (.get bytes fslice-size nslice-size))))
          (doto (.asReadOnlyBuffer region)
            (.get bytes)))
        bytes))))
