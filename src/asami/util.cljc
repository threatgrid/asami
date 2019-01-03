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

#?(:cljs (def raw-lookup {'= = 'not= not= '< < '> > '<= <= '>= >=}))
#?(:cljs (def known-namespaces {'cljs.core (ns-publics 'cljs.core)
                                'clojure.core (ns-publics 'clojure.core)}))

#?(:clj
   (defn fn-for
     "Converts a symbol or string representing an operation into a callable function"
     [op]
     (or (ns-resolve (the-ns 'clojure.core) op)
         (throw (ex-info (str "Unable to resolve symbol '" op " in " (or (namespace op) 'clojure.core))
                         {:op op :namespace (or (namespace op) "clojure.core")}))))

   :cljs
   (defn fn-for
     "Converts a symbol or string representing an operation into a callable function"
     [op]
     (letfn [(resolve-symbol [ns-symbol s] (get (get known-namespaces ns-symbol) (symbol (name s))))]
       (let [op-symbol (if (string? op) (symbol op) op)]
         (or
          (if-let [ons-str (namespace op-symbol)]
            (let [ons-symbol (symbol ons-str)]
              (if-let [ns->functions (known-namespaces ons-symbol)]
                (get ns->functions (symbol (name op-symbol)))
                (when-not (undefined? eval) (eval op-symbol))))
            (or (resolve-symbol 'clojure.core op-symbol)
                (resolve-symbol 'cljs.core op-symbol)))
          (raw-lookup op-symbol)
          (throw (ex-info (str "Unable to resolve symbol '" op-symbol " in " (or (namespace op-symbol) 'cljs.core))
                          {:op op-symbol :namespace (or (namespace op-symbol) "cljs.core")})))))))

