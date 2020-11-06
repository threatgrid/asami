(ns ^{:doc "Common functions for storage based tests"
      :author "Paula Gearon"}
    asami.durable.test-utils
  (:require [asami.durable.block.file.util :as util])
  #?(:clj (:import [java.io File])))

(defn get-filename
  "Returns the resource for creating a manager.
  For Java, this is a java.io.File. On JS this is a string."
  [s]
  #?(:clj (let [f (util/temp-file s)]
            (doto ^File f .delete))
     :cljs s))

