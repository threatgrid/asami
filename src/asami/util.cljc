(ns asami.util
  "The ubiquitous utility namespace that every project seems to have"
  #?(:cljs (:require [cljs.js :as cjs])))

#?(:clj
   (def c-eval clojure.core/eval)

   :cljs
   (defn c-eval
     "Equivalent to clojure.core/eval. Returns nil on error."
     [expr & {:as opts}]
     (try
       (let [def-opts {:eval cjs/js-eval :source-map true :context :expr}
             op (if opts
                  (merge def-opts opts)
                  def-opts)
             {:keys [error value]} (cjs/eval (cjs/empty-state)
                                             expr
                                             op
                                             identity)]
         (if error
           ((.-log js/console) error)
           value))
       (catch :default e ((.-log js/console) e) nil))))
