(ns ^{:doc "Functions for graph analytics in Asami Index graphs"
      :author "Paula Gearon"}
    asami.analytics
  (:require [asami.graph :as graph :refer [Graph GraphType resolve-triple
                                           graph-add new-graph]]
            [asami.common-index :as ci :refer [lowest-level-sets-fn]]
            [clojure.set :as set]
            #?(:clj  [schema.core :as s]
               :cljs [schema.core :as s :include-macros true])))


(defn entity-node?
  "Returns true if a node represents an entity.
   No access to the storage type, so look for namespaced keywords."
  [n]
  (and (keyword? n) (namespace n)))

(s/defn subgraph-from-node :- #{s/Any}
  "Finds a single subgraph for an index graph. Returns all entity IDs that appear
   in the same subgraph as the provided node."
  [{:keys [spo osp] :as graph} :- GraphType
   node :- s/Any]
  (let [get-object-sets-fn (lowest-level-sets-fn graph)]
    (letfn [(next-nodes [seen n]
              (let [down-connected (set/difference
                                    (->> (spo n) vals get-object-sets-fn (apply set/union))
                                    seen)
                    down-connected-entities (set/select entity-node? down-connected)
                    up-connected (->> (osp n) keys (remove seen) set)]
                (set/union down-connected-entities up-connected)))]
      (loop [nodes #{node} seen #{node}]
        (if-not (seq nodes)
          (set/select spo seen)
          (let [next-step (apply set/union (map (partial next-nodes seen) nodes))]
            (recur next-step (set/union seen next-step))))))))

(s/defn subgraph-entities :- [#{s/Any}]
  "Finds subgraph groups for index graphs.
   Each subgraph group is a seq of entity nodes for the nodes in a subgraph."
  [{:keys [spo] :as graph} :- GraphType]
  (letfn [(none-of [graph-sets node]
            (when-not (some #(%1 node) graph-sets) node))]
    (loop [subgraphs [] node (first (keys spo))]
      (if (nil? node)
        subgraphs
        (let [subgraph (set (subgraph-from-node graph node))
              next-subgraphs (conj subgraphs subgraph)
              next-node (some (partial none-of next-subgraphs) (keys spo))]
          (recur next-subgraphs next-node))))))

(s/defn subgraphs :- [GraphType]
  "Returns all subgraphs for a given graph"
  [graph :- GraphType]
  (letfn [(to-graph [entities]
            (let [tx 0
                  edges (->> (resolve-triple graph '?s '?p '?o)
                             (filter (comp entities first)))]
              (reduce (fn [g [s p o]] (graph-add g s p o tx)) (new-graph graph) edges)))]
    (let [groups (subgraph-entities graph)]
      (map to-graph groups))))
