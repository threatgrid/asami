(ns ^{:doc "Macro definintions"
      :author "Paula Gearon"}
    asami.durable.macros
  (:require [asami.durable.common :refer [lock! unlock!]]))

(defmacro with-lock
  "Uses a lock for a block of code"
  [lock & body]
  `(try
     (lock! ~lock)
     ~@body
     (finally (unlock! ~lock))))

(defmacro assert-args
  [& pairs]
  `(do (when-not ~(first pairs)
         (throw (IllegalArgumentException.
                  (str (first ~'&form) " requires " ~(second pairs) " in " ~'*ns* ":" (:line (meta ~'&form))))))
     ~(let [more (nnext pairs)]
        (when more
          (list* `assert-args more)))))

(defmacro with-open*
  "Duplicates the with-open macro from clojure.core."
  [bindings & body]
  (assert-args
   (vector? bindings) "a vector for its binding"
   (even? (count bindings)) "an even number of forms in binding vector")
  (cond
    (= (count bindings) 0) `(do ~@body)
    (symbol? (bindings 0)) `(let ~(subvec bindings 0 2)
                              (try
                                (with-open* ~(subvec bindings 2) ~@body)
                                (finally
                                  (. ~(bindings 0) close))))
    :else (throw (ex-info "with-open only allows Symbols in bindings" {:bindings bindings}))))
