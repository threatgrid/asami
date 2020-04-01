(ns ^{:doc "Converts external data into a graph format (triples)."
      :author "Paula Gearon"}
    writer.entity.writer
  (:require [asami.entity.general :as general :refer [KeyValue EntityMap]]
            [asami.entity.reader :as reader]
            [asami.intern :as intern]
            [schema.core :as s :refer [=>]]
            [clojure.string :as string]
            #?(:clj [clojure.java.io :as io])
            #?(:clj [cheshire.core :as j])
            [naga.store :refer [StorageType]])
  #?(:clj (:import [java.util Map List])))

#?(:clj  (def parse-json-string #(j/parse-string % true))
   :cljs (def parse-json-string #(js->clj (.parse js/JSON %) :keywordize-keys true)))

#?(:clj
   (defn json-generate-string
     ([data] (j/generate-string data))
     ([data indent]
      (j/generate-string
       data
       (assoc j/default-pretty-print-options
              :indentation (apply str (repeat indent \space))))))

   :cljs
   (defn json-generate-string
     ([data] (.stringify js/JSON (clj->js data)))
     ([data indent] (.stringify js/JSON (clj->js data) nil indent))))

(def Triple [(s/one s/Any "Entity")
             (s/one s/Keyword "attribute")
             (s/one s/Any "value")])

(def EntityTriplesPair [(s/one s/Any "node ID")
                        (s/one [Triple] "current list of triples")])

(defn get-asami-first
  "Finds the asami/first property in a map, and gets the value."
  [struct]
  (let [first-pair? (fn [[k v :as p]]
                     (and (= "asami" (namespace k))
                          (string/starts-with? (name k) "first")
                          p))]
    (some first-pair? struct)))

(s/defn containership-triples
  "Finds the list of entity nodes referred to in a list and builds
   triples describing a flat 'contains' property"
  [node :- s/Any
   triples :- [Triple]]
  (let [listmap (->> (group-by first triples)
                     (map (fn [[k vs]]
                            [k (into {} (map #(apply vector (rest %)) vs))]))
                     (into {}))
        node-list (loop [nl [] n node]
                    (if-not n
                      nl
                      (let [{r :asami/rest :as lm} (listmap n)
                            [_ f] (get-asami-first lm)]
                        (recur (conj nl f) r))))]
    (doall  ;; uses a dynamically bound value, so ensure that this is executed
      (map
       (fn [n] [node intern/container-property n])
       node-list))))

(declare value-triples map->triples)

(s/defn list-triples
  "Creates the triples for a list"
  [[v & vs :as vlist]]
  (if (seq vlist)
    (let [node-ref (intern/new-node)
          [value-ref triples] (value-triples v)
          [next-ref next-triples] (list-triples vs)]
      [node-ref (concat [[node-ref intern/data-property value-ref]]
                  (when next-ref [[node-ref :asami/rest next-ref]])
                  triples
                  next-triples)])))

(s/defn value-triples-list :- EntityTriplesPair
  [vlist :- [s/Any]]
  (let [[node triples :as raw-result] (list-triples vlist)]
    (if triples
      [node (concat triples (containership-triples node triples))]
      raw-result)))

(s/defn value-triples
  "Converts a value into a list of triples.
   Return the entity ID of the data coupled with the sequence of triples."
  [v]
  (cond
    (sequential? v) (value-triples-list v)
    (set? v) (value-triples-list (seq v))
    (map? v) (map->triples v)
    (nil? v) nil
    :default [v nil]))

(s/defn property-vals :- [Triple]
  "Takes a property-value pair associated with an entity,
   and builds triples around it"
  [entity-ref :- s/Any
   [property value] :- KeyValue]
  (if-let [[value-ref value-data] (value-triples value)]
    (cons [entity-ref property value-ref] value-data)))


(s/defn map->triples :- EntityTriplesPair
  "Converts a single map to triples. Returns a pair of the map's ID and the triples for the map."
  [data :- {s/Keyword s/Any}]
  (let [entity-ref (or (:db/id data) (intern/new-node))
        triples-data (doall (mapcat (partial property-vals entity-ref)
                                    data))]
    [entity-ref triples-data]))


(s/defn name-for
  "Convert an id (probably a number) to a keyword for identification"
  [id :- s/Any]
  (if (keyword? id)
    id
    (intern/node-label id)))


(s/defn ident-map->triples :- [Triple]
  "Converts a single map to triples for an ID'ed map"
  [j :- EntityMap]
  (let [[node-ref triples] (map->triples j)]
    (if (:db/ident j)
      triples
      (concat [[node-ref :db/ident (name-for node-ref)] [node-ref :asami/entity true]] triples))))


(s/defn entities->triples :- [Triple]
  "Converts parsed JSON into a sequence of triples."
  [j]
  (doall (mapcat ident-map->triples j)))


#?(:clj
    (s/defn stream->triples :- [Triple]
      "Converts a stream to triples"
      [io]
      (with-open [r (io/reader io)]
        (let [data (j/parse-stream r true)]
          (entities->triples data))))
    
   :cljs
    (s/defn stream->triples :- [Triple]
      [io]
      (throw (ex-info "Unsupported IO" {:io io}))))

(s/defn string->triples :- [Triple]
  "Converts a string to triples"
  [s :- s/Str]
  (entities->triples (parse-json-string s)))


;; updating the store

(s/defn existing-triples
  [storage :- StorageType
   node-ref
   [k v]]
  (if-let [subpv (reader/check-structure storage k v)]
    (cons [node-ref k v] (mapcat (partial existing-triples storage v) subpv))
    [[node-ref k v]]))

(s/defn json-update->triples :- [(s/one [Triple] "assertions") (s/one [Triple] "retractions")]
  "Takes a single structure and converts it into triples to be added and triples to be retracted to create a change"
  [storage :- StorageType
   node-ref j]
  (let [pvs (reader/property-values storage node-ref)
        old-node (reader/pairs->struct storage pvs)
        to-remove (remove (fn [[k v]] (if-let [newv (get j k)] (= v newv))) old-node)
        pvs-to-remove (filter (comp (set (map first to-remove)) first) pvs)
        triples-to-remove (mapcat (partial existing-triples storage node-ref) pvs-to-remove)

        to-add (remove (fn [[k v]] (when-let [new-val (get old-node k)] (= new-val v))) j)
        triples-to-add (doall (mapcat (partial property-vals node-ref) to-add))]
    [triples-to-add triples-to-remove]))
