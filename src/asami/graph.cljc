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

(defn plain
  "Converts a transitive-structured predicate into a plain one"
  [pred]
  (let [nm (name pred)]
    (or
     (and (#{\* \+} (last nm))
          (cond (keyword? pred) (keyword (namespace pred) (subs nm 0 (dec (count nm))))
                (symbol? pred) (symbol (namespace pred) (subs nm 0 (dec (count nm))))))
     pred)))

(defn second-last
  "Returns the second-last character in a string."
  [s]
  (let [c (count s)]
    (when (< 1 c)
      (nth s (- c 2)))))

(defn check-for-transitive
  "Tests if a predicate is transitive.
  Returns a plain version of the predicate, along with a value to indicate if the predicate is transitive.
  This value is nil for a plan predicate, or else is a keyword to indicate the kind of transitivity."
  [pred]
  (let [{trans? :trans :as meta-pred} (meta pred)
        not-trans? (and (contains? meta-pred :trans) (not trans?))
        pname (name pred)
        tagged (and (not= \' (second-last pname)) ({\* :star \+ :plus} (last pname)))]
    (if not-trans?
      (when tagged [(plain pred) tagged])
      (if tagged
        [(plain pred) tagged]
        (and trans?
             [pred (get #{:star :plus} trans? :star)])))))
