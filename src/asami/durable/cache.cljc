(ns ^{:doc "Duplicates LRU Cache functionality from clojure.core.cache.
            Duplicated because that cache is not available for ClojureScript
            and the namespace includes JVM specific functionality."
      :author "Paula Gearon"}
    (:require
     #?(:clj [clojure.data.priority-map :as priority-map]
        :cljs [tailrecursion.priority-map :as priority-map])))

(defprotocol CacheProtocol
  "This is the protocol describing the basic cache capability."
  #?(:clj
     (lookup [cache e]
             [cache e not-found]
             "Retrieve the value associated with `e` if it exists, else `nil` in
             the 2-arg case.  Retrieve the value associated with `e` if it exists,
             else `not-found` in the 3-arg case."))
  (has?    [cache e]
   "Checks if the cache contains a value associated with `e`")
  (hit     [cache e]
   "Is meant to be called if the cache is determined to contain a value associated with `e`")
  (miss    [cache e ret]
   "Is meant to be called if the cache is determined to **not** contain a value associated with `e`")
  (evict  [cache e]
   "Removes an entry from the cache")
  (seed    [cache base]
   "Is used to signal that the cache should be created with a seed. The contract is that said cache
   should return an instance of its own type."))

(defn- build-leastness-queue
  [base limit start-at]
  (into (priority-map/priority-map)
        (concat (take (- limit (count base)) (for [k (range (- limit) 0)] [k k]))
                (for [[k _] base] [k start-at]))))

#?(:clj
   (defmacro defcache
     [type-name fields & specifics]
     (let [[base & _] fields
           base-field (with-meta base {:tag 'clojure.lang.IPersistentMap})]
       `(deftype ~type-name [~@fields]
          ~@specifics

          clojure.lang.ILookup
          (valAt [this# key#]
            (lookup this# key#))
          (valAt [this# key# not-found#]
            (if (has? this# key#)
              (lookup this# key#)
              not-found#))

          java.lang.Iterable
          (iterator [_#]
            (.iterator ~base-field))

          clojure.lang.IPersistentMap
          (assoc [this# k# v#]
            (miss this# k# v#))
          (without [this# k#]
            (evict this# k#))

          clojure.lang.Associative
          (containsKey [this# k#]
            (has? this# k#))
          (entryAt [this# k#]
            (when (has? this# k#)
              (clojure.lang.MapEntry. k# (lookup this# k#))))

          clojure.lang.Counted
          (count [this#]
            (count ~base-field))

          clojure.lang.IPersistentCollection
          (cons [this# elem#]
            (seed this# (conj ~base-field elem#)))
          (empty [this#]
            (seed this# (empty ~base-field)))
          (equiv [this# other#]
            (= other# ~base-field))

          clojure.lang.Seqable
          (seq [_#]
            (seq ~base-field)))))

   :cljs
   (defmacro defcache
     [type-name fields & specifics]
     (let [[base-field & _] fields]
       `(deftype ~type-name [~@fields]
          ~@specifics

          ILookup
          (-lookup [this# key#]
            (get ~base-field key#))
          (-lookup [this# key# not-found#]
            (if (has? this# key#)
              (get ~base-field key#)
              not-found#))

          IIterable
          (-iterator [_#]
            (-iterator ~base-field))

          IAssociative
          (-assoc [this# k# v#]
            (miss this# k# v#))
          (-containsKey [this# k#]
            (has? this# k#))

          IMap
          (-dissoc [this# k#]
            (evict this# k#))

          ICounted
          (-count [this#]
            (count ~base-field))

          IEmptyableCollection
          (-empty [this#]
            (seed this# (empty ~base-field)))

          IEquiv
          (-equiv [this# other#]
            (= other# ~base-field))

          ISeqable
          (-seq [_#]
            (seq ~base-field))))))


(defcache LRUCache [cache lru tick limit]
  CacheProtocol
  #?(:clj
     (lookup [_ item]
             (get cache item)))
  #?(:clj
     (lookup [_ item not-found]
             (get cache item not-found)))
  (has? [_ item]
        (contains? cache item))
  (hit [_ item]
       (let [tick+ (inc tick)]
         (LRUCache. cache
                    (if (contains? cache item)
                      (assoc lru item tick+)
                      lru)
                    tick+
                    limit)))
  (miss [_ item result]
        (let [tick+ (inc tick)]
          (if (>= (count lru) limit)
            (let [k (if (contains? lru item)
                      item
                      (first (peek lru))) ;; minimum-key, maybe evict case
                  c (-> cache (dissoc k) (assoc item result))
                  l (-> lru (dissoc k) (assoc item tick+))]
              (LRUCache. c l tick+ limit))
            (LRUCache. (assoc cache item result) ;; no change case
                       (assoc lru item tick+)
                       tick+
                       limit))))
  (evict [this key]
         (if (contains? cache key)
           (LRUCache. (dissoc cache key)
                      (dissoc lru key)
                      (inc tick)
                      limit)
           this))
  (seed [_ base]
        (LRUCache. base
                   (build-leastness-queue base limit 0)
                   0
                   limit))
  Object
  (toString [_]
            (str cache \, \space lru \, \space tick \, \space limit)))


(defn lru-cache-factory
  "Returns an LRU cache with the cache and usage-table initialied to `base` --
   each entry is initialized with the same usage value.
   This function takes an optional `:threshold` argument that defines the maximum number
   of elements in the cache before the LRU semantics apply (default is 32)."
  [base & {threshold :threshold :or {threshold 32}}]
  {:pre [(number? threshold) (< 0 threshold)
         (map? base)]}
  (seed (LRUCache. {} (priority-map/priority-map) 0 threshold) base))
