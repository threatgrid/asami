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
            [asami.storage :as storage]))

(defn -connect
  "Connects to a database described in the uri"
  [^String uri]
  (core/connect uri))

(defn -createDatabase ^Boolean
  "Creates a database described in the uri"
  [^String uri]
  (core/create-database uri))

(defn -deleteDatabase ^Boolean
  "Removes a database of the given uri"
  [^String uri]
  (core/delete-database uri))

(defn -getDatabaseNames ^java.util.List
  "Retrieves the names of the known databases"
  []
  (core/get-database-names))

(defn -q
  "Executes a query against inputs"
  [query inputs]
  (apply core/q query inputs))

(defonce ids (atom {}))

(defn -tempid
  "Generates a temporary internal ID"
  ([])
  ([connection] (storage/new-node connection))
  ([connection id]
   (let [id-for (fn [m i]
                  (if (contains? m i)
                    m
                    (assoc m i (storage/new-node connection))))
         ident (str (storage/name connection) id)]
     (get (swap! ids id-for ident) ident))))
