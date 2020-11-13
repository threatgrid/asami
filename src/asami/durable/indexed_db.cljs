(ns asami.durable.indexed-db
  (:require [asami.durable.decoder :as decoder]
            [asami.durable.encoder :as encoder]
            [asami.bytes :as bytes]
            [asami.durable.common :as common]))

(def PAGE_MAX_BYTE_LENGTH
  ;; 16.777214999999998 MB
  0X00FFFFFF)

(def REGION_BYTE_LENGTH
  ;; 1.118481 MB
  0x00111111)

(def PAGE_MAX_REGIONS
  #_ (/ PAGE_MAX_BYTE_LENGTH REGION_BYTE_LENGTH)
  0X0000000F)

(def MAX_PAGES
  0xffffffff)

(defn region-length [page]
  (/ (bytes/byte-length page) REGION_BYTE_LENGTH))

(defn allocate-region
  "Return a copy of the ArrayBuffer page with an additional region
  allocated or nil if page already has `PAGE_MAX_REGIONS` allocated."
  [page]
  (let [number-of-regions (region-length page)]
    (when (< number-of-regions PAGE_MAX_REGIONS)
      (let [new-number-of-regions (inc number-of-regions)
            new-page-byte-length (* new-number-of-regions REGION_BYTE_LENGTH)]
        (bytes/transfer page new-page-byte-length)))))

(defn make-page
  "Return a page, an ArrayBuffer, with one region preallocated."
  []
  (js/ArrayBuffer. REGION_BYTE_LENGTH))

(defn read-id
  "Return a pair of numbers, page-number and offset, from the number
  id."
  [id]
  (let [id-bytes (bytes/from-number id)
        id-view  (js/DataView. id-bytes)
        page (.getUint32 id-view 0)
        offset (.getUint32 id-view 4)]
    [page offset]))

(defn set-id-offset
  "Return a new id with offset set to new-offset."
  [id new-offset]
  ;; Enforce offset is not beyond `PAGE_MAX_BYTE_LENGTH`.
  (let [id-bytes (bytes/from-number id)
        id-view  (js/DataView. id-bytes)]
    (.setUint32 id-view 4 new-offset)
    (.getFloat64 id-view)))

(defn make-id [page-number offset]
  (let [id-bytes (bytes/byte-array 8)
        id-view  (js/DataView. id-bytes)]
    (.setUint32 id-view 0 page-number)
    (.setUint32 id-view 4 offset)
    (.getFloat64 id-view)))

(defn empty-store []
  {:id (make-id 0 0)
   :pages {0 (make-page)}
   :ids []})

(defn write-object
  [{:keys [id pages ids] :as store} value]
  (let [[header data] (encoder/to-bytes value)
        encoded-byte-length (+ (bytes/byte-length header)
                               (bytes/byte-length data))
        [page-number offset] (read-id id)]
    (if (<= (+ offset encoded-byte-length) PAGE_MAX_BYTE_LENGTH)
      ;; There is enough room to write the data on the current page.
      (let [page (get pages page-number)
            byte-buffer (.. (bytes/byte-buffer page)
                            (position offset)
                            (put header)
                            (put data))
            new-offset (.position byte-buffer)
            new-id (set-id-offset id new-offset)]
        (merge store {:id new-id
                      :ids (conj (vec ids) id)}))
      ;; There is not enough room to write the data on the current page.
      (let [new-page-number (inc page-number)]
        (if (< new-page-number MAX_PAGES)
          ;; There is enough address space for a new page.
          (let [new-page (make-page)
                byte-buffer (.. (bytes/byte-buffer new-page)
                                (position offset)
                                (put header)
                                (put data))
                new-offset (.position byte-buffer)
                new-id (make-id new-page-number new-offset)]
            (merge store {:pages (assoc pages new-page-number new-page)
                          :id new-id
                          :ids (conj (vec ids) (make-id new-page 0))}))
          ;; There is not enough address space for a new page.
          (throw (ex-info "Out of address space" {})))))))

(defn array-buffer-paged-reader [array-buffer]
  (let [data-view (js/DataView. array-buffer)]
    (reify
      common/Paged
      (refresh! [this]
        nil)

      (read-byte [this offset]
        (.getInt8 data-view offset))

      (read-short [this offset]
        (.getInt16 data-view offset))

      (read-bytes [this offset length]
        (.slice (.-buffer data-view) offset (+ offset length)))

      (read-bytes-into [this offset bytes]
        (.set bytes (.slice (.-buffer data-view) offset))))))

(defn get-object [store id]
  (let [[page-number offset] (read-id id)
        pages (get store :pages) 
        page (get pages page-number)
        reader (array-buffer-paged-reader page)]
    (decoder/read-object reader offset)))

(defn array-buffer-store []
  (let [store (atom (empty-store))]
    (reify
      IDeref
      (-deref [this]
        (deref store))

      common/FlatStore
      (write-object! [this object]
        (let [{:keys [ids]} (swap! store write-object object)
              id (peek ids)]
          id))

      (get-object [this id]
        (get-object (deref store) id)))))

(defn fetch-pages-promise
  [idb]
  (js/Promise.
   (fn [resolve reject]
     (let [pages-tx (.transaction idb #js ["pages"] "readonly")
           pages-request (.getAll (.objectStore pages-tx "pages"))]
       (set! (.-error pages-request)
             (fn [event]
               (reject (ex-info "Page request failed" {:event event}))))
       (set! (.-onsuccess pages-request)
             (fn [event]
               (let [page-objects (.-result (.-target event))
                     pages (if (seq page-objects)
                             (into {} (map (fn [page]
                                             [(.-id page) (.-page page)]))
                                   page-objects)
                             {0 (make-page)})]
                 (resolve pages))))))))

(defn fetch-next-id-promise
  [idb]
  (js/Promise.
   (fn [resolve reject]
     (let [meta-tx (.transaction idb #js ["meta"] "readonly")
           id-request (.get (.objectStore meta-tx "meta") "id")]
       (set! (.-onerror id-request)
             (fn [event]
               (reject (ex-info "Request for next id failed" {:event event}))))
       (set! (.-onsuccess id-request)
             (fn [event]
               (let [id-result (.-result (.-target event))]
                 (if (some? id-result)
                   (resolve (.-value id-result))
                   (resolve (make-id 0 0))))))))))



(defn create-meta-object-store-promise
  [idb]
  (js/Promise.
   (fn [resolve reject]
     (try
       (let [meta-object-store (.createObjectStore idb "meta" #js {:keyPath "key"})]
         (.createIndex meta-object-store "metaIndex" "key" #js {:unique true}))
       (catch js/DOMException e
         (if (= "ConstraintError" (.-name e))
           (resolve idb)
           (reject (ex-info "Could not create object store for pages" {:error e}))))))))

(defn create-pages-object-store-promise
  [idb]
  (js/Promise.
   (fn [resolve reject]
     (try
       (let [page-object-store (.createObjectStore idb "pages" #js {:keyPath "id"})]
         (.createIndex page-object-store "idIndex" "id" #js {:unique true}))
       (catch js/DOMException e
         (if (= "ConstraintError" (.-name e))
           (resolve idb)
           (reject (ex-info "Could not create object store for pages" {:error e}))))))))

(defn idb-open-promise
  [database-name]
  (js/Promise.
   (fn [resolve reject]
     (let [idb-request (.open (.-indexedDB js/window) database-name 2)]
       (set! (.-onerror idb-request)
             (fn [event]
               (reject (ex-info (str "Error loading database " (pr-str database-name))
                                {:event event}))))
       (set! (.-onsuccess idb-request)
             (fn [event]
               (resolve (.-result idb-request))))

       (set! (.-onupgradeneeded idb-request)
             (fn [event]
               (this-as *this*
                 (let [idb (.-result *this*)]
                   (.. (create-meta-object-store-promise)
                       (then (fn [_]
                               (.. (create-pages-object-store-promise)
                                   (then resolve)
                                   (catch reject))))
                       (catch reject))))))

       (set! (.-onversionchange idb-request)
             (fn [event]
               (reject (ex-info "Not Implemented: IndexedDB version change" {:event event}))))))))


(defn indexed-db-store [database-name]
  (let [state (atom (ex-info "Connection pending" {:database-name database-name}))
        resolve (fn [data]
                  (reset! state data))
        reject (fn [error]
                 (reset! state error))]
    (.. (idb-open-promise database-name)
        (then (fn [idb]
                (.. (fetch-next-id-promise idb)
                    (then (fn [next-id]
                            (.. (fetch-pages-promise idb)
                                (then (fn [pages]
                                        (resolve {:idb idb
                                                  :id next-id
                                                  :pages pages})))
                                (catch reject))))
                    (catch reject))))
        (catch reject))
    (reify
      IDeref
      (-deref [this]
        (deref state))

      common/Closeable
      (force! [this]
        (let [value (deref state)]
          (if (map? value)
            (let [{:keys [idb id pages]} value
                  transaction (.transaction idb #js ["meta" "pages"] "readwrite")
                  meta-store (.objectStore transaction "meta")
                  pages-store (.objectStore transaction "pages")]
              (.put meta-store #js {:key "id" :value id})
              (doseq [[page-id page] pages]
                (.put pages-store #js {:id page-id, :page page})))
            (throw value))))

      common/FlatStore
      (write-object! [this object]
        (let [value (deref state)]
          (if (map? value)
            (let [{:keys [ids]} (swap! state write-object object)
                  id (peek ids)]
              id)
            (throw value))))

      (get-object [this id]
        (let [value (deref state)]
          (if (map? value)
            (get-object value id)
            (throw value)))))))

(comment
  (def bar
    (indexed-db-store "bar"))

  (deref bar)
  ;; => {:idb #object[IDBDatabase [object IDBDatabase]], :id 0, :pages {0 #object[ArrayBuffer [object ArrayBuffer]]}}

  (def id-1
    (common/write-object! bar "shoes"))

  (def id-2
    (common/write-object! bar :food))

  (def id-3
    (common/write-object! bar :clothes))

  (def id-4
    (common/write-object! bar "dinner"))

  (common/get-object bar id-1)
  ;; => "shoes"
  (common/get-object bar id-2)
  ;; => :food
  (common/get-object bar id-3)
  ;; => :clothes
  (common/get-object bar id-4)
  ;; => "dinner"
  (common/force! bar)

  (def bar-2
    (indexed-db-store "bar"))

  (deref bar-2)
  ;; => {:idb #object[IDBDatabase [object IDBDatabase]], :id 1.3e-322, :pages {0 #object[ArrayBuffer [object ArrayBuffer]]}}

  (common/get-object bar-2 id-1)
  ;; => "shoes"
  (common/get-object bar-2 id-2)
  ;; => :food
  (common/get-object bar-2 id-3)
  ;; => :clothes
  (common/get-object bar-2 id-4)
  ;; => "dinner"
  )
