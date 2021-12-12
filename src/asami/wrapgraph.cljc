(ns ^{:doc "An graph wrapper for seqs"
      :author "Paula Gearon"}
    asami.wrapgraph
  (:require [asami.graph :as gr :refer [Graph graph-add graph-delete graph-diff resolve-triple count-triple]]
            [asami.common-index :as common :refer [? NestedIndex]]
            [asami.analytics :as analytics]
            [zuko.node :refer [NodeAPI]]
            [zuko.logging :as log :include-macros true]
            [schema.core :as s :include-macros true]))

(def ^:const TX-LOGGING "asami.tx.logging")

(defmulti get-from-index
  "Lookup an index in the graph for the requested data.
   Returns a sequence of unlabelled bindings. Each binding is a vector of binding values."
  common/simplify)

(defmethod get-from-index [:v :v :v]
  [data s p o]
  (if (some (fn [[a b c]] (and (= s a) (= p b) (= o c))) data) [[]] []))

(defmethod get-from-index [:v :v  ?]
  [data s p o]
  (sequence
   (comp
    (filter (fn [[a b _]] (and (= s a) (= p b))))
    (map #(subseq % 2 3)))
   data))

(defmethod get-from-index [:v  ? :v]
  [data s p o]
  (sequence
   (comp
    (filter (fn [[a _ c]] (and (= s a) (= o c))))
    (map #(subseq % 1 2)))
   data))

(defmethod get-from-index [:v  ?  ?]
  [data s p o]
  (sequence
   (comp
    (filter (fn [[a _ _]] (= a s)))
    (map #(subseq % 1 3)))
   data))

(defmethod get-from-index [ ? :v :v]
  [data s p o]
  (sequence
   (comp
    (filter (fn [[_ b c]] (and (= p b) (= o c))))
    (map #(subseq % 0 1)))
   data))

(defmethod get-from-index [ ? :v  ?]
  [data s p o]
  (sequence
   (comp
    (filter (fn [[_ b _]] (= b p)))
    (map #(vector (first %) (nth % 2))))
   data))

(defmethod get-from-index [ ?  ? :v]
  [data s p o]
  (sequence
   (comp
    (filter (fn [[_ _ c]] (= c o)))
    (map #(subseq % 0 2)))
   data))

(defmethod get-from-index [ ?  ?  ?]
  [data s p o]
  (map #(subseq % 0 3) data))

(defn exists?
  "Fast lookup for a statement in an index/graph"
  [graph s p o]
  (some-> (:spo graph) s p o))

(defn graphs-equiv?
  "Checks if two GraphWrapper instances are equivalent"
  [g1 g2]
  (and (identical? (:addgraph g1) (:addgraph g2))
       (identical? (:delgraph g1) (:delgraph g2))))

(defn graph-subtract
  [a b]
  (reduce (fn [g [s p o]] (gr/delete g s p o)) a (gr/resolve-triple b ?s ?p ?o)))

(defrecord GraphWrapper [addgraph delgraph wrapped-graph tx-logging]
  Graph
  (new-graph [this] (gr/new-graph wrapped-graph))

  (graph-add [this subj pred obj]
    (graph-add this subj pred obj gr/*default-tx-id*))

  (graph-add [this subj pred obj tx]
    (log/trace "wrap insert: " [subj pred obj tx])
    (if (seq (gr/resolve-triple delgraph subj pred obj))
      ;; deleted statements can just be undeleted. They keep their old statement ID
      (assoc this :delgraph (gr/graph-delete delgraph subj pred obj))
      ;; only check if a statement exists if transactions are being logged
      (if (and tx-logging (seq (gr/resolve-triple wrapped-graph subj pred obj)))
        ;; the statement exists in the wrapped storage. Don't reinsert
        this
        ;; add the statement to in-memory storage
        (assoc this :addgraph (gr/graph-add addgraph subj pred obj tx)))))

  (graph-delete [this subj pred obj]
    (log/trace "delete " [subj pred obj])
    (if (seq (gr/resolve-triple addgraph subj pred obj))
      ;; added statements can be removed from the in-memory graph
      (assoc this :addgraph (gr/graph-delete addgraph subj pred obj))
      ;; only check if a statement already exists if transactions are being logged
      (if (and tx-logging (seq (gr/resolve-triple wrapped-graph subj pred obj)))
        ;; the statement does not exist in wrapped storage. Don't retract it.
        this
        ;; remove the statement from in-memory storage. TX ids are not relevant on the delgraph
        (assoc this :delgraph (gr/graph-add delgraph subj pred obj nil)))))

  (graph-transact [this tx-id assertions retractions]
    (gr/graph-transact this tx-id assertions retractions (volatile! [[] [] {}])))

  (graph-transact
    [this tx-id assertions retractions generated-data]
    (let [[a r] @generated-data
          asserts (transient a)
          retracts (transient r)
          change-graph (fn [gr rm-key add-key]
                         (fn [acc [s p o]]
                           (if (exists? gr s p o)
                             (assoc acc rm-key (gr/graph-delete (rm-key acc) s p o))
                             (assoc acc add-key (gr/graph-add (add-key acc) s p o tx-id)))))
          graph-remove (change-graph addgraph :addgraph :delgraph)
          graph-add (change-graph delgraph :delgraph :addgraph)
          new-graph-del (reduce (if tx-logging
                                  graph-remove
                                  (fn [acc [s p o]]
                                    (let [aa (graph-remove acc s p o)]
                                      (when-not (graphs-equiv? aa acc)
                                        (conj! asserts (->Datom s p o tx-id false)))
                                      aa)))
                                this retractions)
          new-graph (reduce (if tx-logging
                              graph-add
                              (fn [acc [s p o]]
                                (let [aa (graph-add acc s p o)]
                                  (when-not (graphs-equiv? aa acc)
                                    (conj! asserts (->Datom s p o tx-id true)))
                                  aa)))
                            new-graph-del assertions)]
      (vreset! generated-data (if tx-logging
                                [(persistent! asserts) (persistent! retracts)]
                                [(map (fn [[s p o]] (->Datom s p o true)) assertions)
                                 (map (fn [[s p o]] (->Datom s p o false)) retractions)]))
      new-graph))

  (graph-diff [this other]
    (if (= other wrappedgraph)
      (merge-graphs addgraph delgraph)
      (throw (ex-info "Unsupported operation" {:operation :graph-diff}))))

  (resolve-triple [this subj pred obj]
    (if-let [[plain-pred trans-tag] (common/check-for-transitive pred)]
      ;; TODO: this is very important to add
      (throw (ex-info "Unsupported operation" {:operation :transitive-resolve-triple}))
      (let [removed? (set (resolve-triple delgraph subj pred obj))]
        (concat
         (resolve-triple addgraph subj pred obj)
         (remove removed? (resolve-triple wrapped-graph subj pred obj))))))

  (count-triple [this subj pred obj]
    (- (+ (count (resolve-triple wrapped-graph subj pred obj))
          (count (resolve-triple addgraph subj pred obj)))
       (count (resolve-triple delgraph subj pred obj)))))

(defn get-logging
  []
  #?(:clj (some->> (System/getProperty TX-LOGGING) str/lower-case (= "true"))
     :cljs nil))

(defn wrap-graph
  "Wraps a given graph with in-memory operations"
  [existing-graph]
  (->GraphSeq (mem/new-graph) (mem/new-graph)
              (get-logging) existing-graph))

(defn unwrap-graph
  "Merges a wrapped graph with the in-memory data used to wrap it."
  [{:keys [addgraph delgraph wrapped-graph tx-logging] :as graph}]
  (gr/graph-transact wrapped-graph
                     (gr/resolve-triple addgraph '?s '?p '?o)
                     (gr/resolve-triple delgraph '?s '?p '?o)))
