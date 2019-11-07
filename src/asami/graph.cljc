(ns ^{:doc "The graph index API."
      :author "Paula Gearon"}
    asami.graph)


(defprotocol Graph
  (graph-add [this subj pred obj] "Adds triples to the graph")
  (graph-delete [this subj pred obj] "Removes triples from the graph")
  (graph-diff [this other] "Returns all subjects that have changed in this graph, compared to other")
  (resolve-triple [this subj pred obj] "Resolves patterns from the graph, and returns unbound columns only")
  (count-triple [this subj pred obj] "Resolves patterns from the graph, and returns the size of the resolution"))

(defn resolve-pattern
  "Convenience function to extract elements out of a pattern to query for it"
  [graph [s p o :as pattern]]
  (resolve-triple graph s p o))

(defn count-pattern
  "Convenience function to extract elements out of a pattern to count the resolution"
  [graph [s p o :as pattern]]
  (count-triple graph s p o))

(defn transitive?
  "Tests if a predicate is transitive. Generally, this is determined by a * character at the end
   of a symbol or keyword name, but it may be overridden by metadata containing an entry of:
   {:trans truthy/falsy}
   Returns a truthy value"
  [pred]
  (let [{trans? :trans :as meta-pred} (meta pred)
        not-trans? (and (contains? meta-pred :trans) (not trans?))
        star? (= \* (last (name pred)))]
    (if star? (not not-trans?) trans?)))
