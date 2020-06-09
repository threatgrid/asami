(ns ^{:doc "Common internal elements of storage"
      :author "Paula Gearon"}
    asami.internal
  (:require [schema.core :as s :refer [=>]]
            [clojure.string :as string]
            #?(:cljs [cljs.core :refer [Symbol]])
            [asami.graph :as graph])
  #?(:clj (:import [clojure.lang Symbol])))


(def data-property :naga/first)

(def container-property :naga/contains)

(defn new-node
  "Create a new node"
  []
  (->> "node-"
       gensym
       name
       (keyword "mem")))

(defn node-id
  "Retrieve the unique ID of a node"
  [n]
  (subs (name n) 5))

(defn node-type?
  "Test if the presented object is an internal node"
  [value]
  (and (keyword? value)
       (= "mem" (namespace value))
       (string/starts-with? (name value) "node-")))

(defn node-label
  "Returns a keyword label for a node"
  [n]
  (keyword "naga" (str "id-" (node-id n))))


(def project-args {:new-node new-node
                   :node-label node-label})

(defn project-ins-args
  [graph]
  (assoc project-args
         :resolve-pattern (partial graph/resolve-pattern graph)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Type definitions for projection

(def EntityPropertyElt
  (s/cond-pre s/Keyword s/Symbol #?(:clj Long :cljs s/Num)))

;; simple pattern containing a single element. e.g. [?v]
(def EntityPattern [(s/one s/Symbol "entity")])

;; two or three element pattern.
;; e.g. [?s :property]
;;      [:my/id ?property ?value]
(def EntityPropertyPattern
  [(s/one EntityPropertyElt "entity")
   (s/one EntityPropertyElt "property")
   (s/optional s/Any "value")])

;; The full pattern definition, with 1, 2 or 3 elements
(def EPVPattern
  (s/if #(= 1 (count %))
    EntityPattern
    EntityPropertyPattern))

(def Value (s/pred (complement symbol?) "Value"))

(def Results [[Value]])

(def EntityPropAxiomElt
  (s/cond-pre s/Keyword #?(:clj Long :cljs s/Num)))

(def EntityPropValAxiomElt
  (s/conditional (complement symbol?) s/Any))

(def Axiom
  [(s/one EntityPropAxiomElt "entity")
   (s/one EntityPropAxiomElt "property")
   (s/one EntityPropValAxiomElt "value")])

(s/defn vartest? :- s/Bool
  [x]
  (and (symbol? x) (boolean (#{\? \%} (first (name x))))))

(s/defn vars :- [s/Symbol]
  "Return a seq of all variables in a pattern"
  [pattern :- EPVPattern]
  (filter vartest? pattern))

