(ns ^{:doc "A storage implementation over in-memory indexing. Includes full query engine."
      :author "Paula Gearon"}
    asami.core
    (:require [clojure.string :as str]
              [asami.index :as mem]
              [asami.query :as query]
              [naga.storage.store-util :as store-util]
              [naga.store :as store :refer [Storage StorageType]]
              [naga.store-registry :as registry]
              #?(:clj  [schema.core :as s]
                 :cljs [schema.core :as s :include-macros true])
              #?(:clj [clojure.core.cache :as c])))


#?(:clj
  ;; Using a cache of 1 is currently redundant to an atom
  (let [m (atom (c/lru-cache-factory {} :threshold 1))]
    (defn get-count-fn
      "Returns a memoized counting function for the current graph.
       These functions only last as long as the current graph."
      [graph]
      (if-let [f (c/lookup @m graph)]
        (do
          (swap! m c/hit graph)
          f)
        (let [f (memoize #(mem/count-pattern graph %))]
          (swap! m c/miss graph f)
          f))))

  :cljs
  (let [m (atom {})]
    (defn get-count-fn
      "Returns a memoized counting function for the current graph.
       These functions only last as long as the current graph."
      [graph]
      (if-let [f (get @m graph)]
        f
        (let [f (memoize #(mem/count-pattern graph %))]
          (reset! m {graph f})
          f)))))

(defrecord MemoryStore [before-graph graph]
  Storage
  (start-tx [this] (->MemoryStore graph graph))

  (commit-tx [this] this)

  (deltas [this]
    ;; sort responses by the number in the node ID, since these are known to be ordered
    (when-let [previous-graph (or (:data (meta this)) before-graph)]
      (->> (mem/graph-diff graph previous-graph)
           (filter (fn [s] (seq (mem/resolve-pattern graph [s :naga/entity '?]))))
           (sort-by #(subs (name %) 5)))))

  (new-node [this]
    (->> "node-"
        gensym
        name
        (keyword "mem")))

  (node-id [this n]
    (subs (name n) 5))

  (node-type? [this prop value]
    (and (keyword? value)
         (= "mem" (namespace value))
         (str/starts-with? (name value) "node-")))

  (data-property [_ data]
    :naga/first)

  (container-property [_ data]
    :naga/contains)

  (resolve-pattern [_ pattern]
    (mem/resolve-pattern graph pattern))

  (count-pattern [_ pattern]
    (if-let [count-fn (get-count-fn graph)]
      (count-fn pattern)
      (mem/count-pattern graph pattern)))

  (query [this output-pattern patterns]
    (store-util/project this output-pattern (query/join-patterns graph patterns)))

  (assert-data [_ data]
    (->MemoryStore before-graph (query/add-to-graph graph data)))

  (assert-schema-opts [this _ _] this)

  (query-insert [this assertion-patterns patterns]
    (letfn [(ins-project [data]
              (let [cols (:cols (meta data))]
                (store-util/insert-project this assertion-patterns cols data)))]
      (->> (query/join-patterns graph patterns)
           ins-project
           (query/add-to-graph graph)
           (->MemoryStore before-graph)))))

(def empty-store (->MemoryStore nil mem/empty-graph))

(s/defn create-store :- StorageType
  "Factory function to create a store"
  [config]
  empty-store)

(registry/register-storage! :memory create-store)

(s/defn q
  [query & inputs]
  (let [{:keys [find in with where]} (query/query-map query)
        store (or (some (partial extends? Storage)) empty-store)
        graph (or (:graph store) mem/empty-graph)]
    ;; TODO: read :in for bindings as provide as a part-result to join-patterns
    (store-util/project store find (query/join-patterns graph where))))
