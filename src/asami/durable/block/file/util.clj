(ns ^{:doc "Utilities for file access"
      :author "Paula Gearon"}
  asami.durable.block.file.util
  (:import [java.io File]))

(defn temp-dir
  "Gets the temporary directory, or the current directory if none is found."
  [] (System/getProperty "java.io.tmpdir" "."))

(defn temp-file
  "Returns a File refering to a temporary path, that has not been created."
  [nm] (File. (temp-dir) nm))

