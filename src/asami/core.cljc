(ns ^{:doc "A storage implementation over in-memory indexing. Includes full query engine."
      :author "Paula Gearon"}
    asami.core
    (:require [clojure.string :as str]
              [asami.graph :as gr]
              [asami.index :as mem]
              [asami.multi-graph :as multi]
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
        (let [f (memoize #(gr/count-pattern graph %))]
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
        (let [f (memoize #(gr/count-pattern graph %))]
          (reset! m {graph f})
          f)))))

(defn shorten
  "truncates a symbol or keyword to exclude a ' character"
  [a]
  (if (string? a)
    a
    (let [nns (namespace a)
          n (name a)
          cns (cond
                (symbol? a) symbol
                (keyword? a) keyword
                :default (throw (ex-info "Invalid attribute type in rule head" {:attribute a :type (type a)})))]
      (cns nns (subs n 0 (dec (count n)))))))

(declare ->MemoryStore)

(defrecord MemoryStore [before-graph graph]
  Storage
  (start-tx [this] (->MemoryStore graph graph))

  (commit-tx [this] this)

  (deltas [this]
    ;; sort responses by the number in the node ID, since these are known to be ordered
    (when-let [previous-graph (or (:data (meta this)) before-graph)]
      (->> (gr/graph-diff graph previous-graph)
           (filter (fn [s] (seq (gr/resolve-pattern graph [s :naga/entity '?]))))
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
    (gr/resolve-pattern graph pattern))

  (count-pattern [_ pattern]
    (if-let [count-fn (get-count-fn graph)]
      (count-fn pattern)
      (gr/count-pattern graph pattern)))

  (query [this output-pattern patterns]
    (store-util/project this output-pattern (query/join-patterns graph patterns nil)))

  (assert-data [_ data]
    (->MemoryStore before-graph (query/add-to-graph graph data)))

  (retract-data [_ data]
    (->MemoryStore before-graph (query/delete-from-graph graph data)))

  (assert-schema-opts [this _ _] this)

  (query-insert [this assertion-patterns patterns]
    ;; convert projection patterns to output form
    ;; remember which patterns were converted
    (let [[assertion-patterns'
           update-attributes] (reduce (fn [[pts upd] [e a v :as p]]
                                        (if (or (str/ends-with? (name a) "'") (:update (meta p)))
                                          (let [short-a (shorten a)]
                                            [(conj pts [e short-a v]) (conj upd short-a)])
                                          [(conj pts p) upd]))
                                      [[] #{}]
                                      patterns)
          ins-project (fn [data]
                        (let [cols (:cols (meta data))]
                          (store-util/insert-project this assertion-patterns' cols data)))
          lookup-triple (fn [part-pattern]
                          (let [pattern (conj part-pattern '?v)
                                values (gr/resolve-pattern graph pattern)]
                            (map (partial conj part-pattern) values)))
          is-update? #(update-attributes (nth % 1))
          additions (ins-project (query/join-patterns graph patterns nil))
          removals (->> additions
                        (filter is-update?)
                        (map #(vec (take 2 %)))
                        (mapcat lookup-triple))]
      (->MemoryStore before-graph
                     (-> graph
                         (query/delete-from-graph removals)
                         (query/add-to-graph additions))))))

(def empty-store (->MemoryStore nil mem/empty-graph))

(def empty-multi-store (->MemoryStore nil multi/empty-multi-graph))

(defn update-store
  [{:keys [before-graph graph]} f & args]
  (->MemoryStore before-graph (apply f graph args)))

(s/defn create-store :- StorageType
  "Factory function to create a store"
  [config]
  empty-store)

(s/defn create-multi-store :- StorageType
  "Factory function to create a multi-graph-store"
  [config]
  empty-multi-store)

(registry/register-storage! :memory create-store)
(registry/register-storage! :memory-multi create-multi-store)

(s/defn graph->store :- StorageType
  "Wraps a graph in the Storage record"
  [graph :- gr/GraphType]
  (->MemoryStore nil graph))

(s/defn q
  [query & inputs]
  (query/query-entry query empty-store mem/empty-graph inputs))
