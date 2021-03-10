(ns ^{:doc "Main entry point for Java."}
    asami.peer
  (:gen-class
   :name asami.Peer
   :methods [#^{:static true} [connect [String] Object]
             #^{:static true} [createDatabase [String] boolean]
             #^{:static true} [deleteDatabase [String] boolean]
             #^{:static true} [getDatabaseNames [] java.util.List]
             #^{:static true} [q [Object] Object]
             #^{:static true} [tempid [Object] Object]
             #^{:static true} [tempid [Object long] Object]
             #^{:static true} [tempid [] Object]])
  (:require [asami.core :as core]
            [asami.graph :as graph]
            [asami.storage :as storage]
            [zuko.node :refer [new-node]]
            [asami.storage :as storage]))

(defn -connect
  "Connects to a database described in the uri"
  [^String uri]
  (core/connect uri))

(defn -createDatabase
  "Creates a database described in the uri"
  ^Boolean [^String uri]
  (core/create-database uri))

(defn -deleteDatabase
  "Removes a database of the given uri"
  ^Boolean [^String uri]
  (core/delete-database uri))

(defn -getDatabaseNames
  "Retrieves the names of the known databases"
  ^java.util.List []
  (core/get-database-names))

(defn -q
  "Executes a query against inputs"
  [query inputs]
  (apply core/q query inputs))

(defonce ids (atom {}))

(defn -tempid
  "Generates a temporary internal ID"
  ([] (graph/new-node))
  ([connection] (new-node (storage/graph (storage/db connection))))
  ([connection id]
   (let [id-for (fn [m i]
                  (if (contains? m i)
                    m
                    (assoc m i (-tempid connection))))
         ident (str (storage/get-name connection) id)]
     (get (swap! ids id-for ident) ident))))
