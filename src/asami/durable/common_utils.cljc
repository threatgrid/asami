(ns ^{:doc "A common namespace for utility functions for storage. "
      :author "Paula Gearon"}
    asami.durable.common-utils
    (:require [clojure.string :as string]
              #?(:clj [asami.durable.block.file.block-file :as block-file])
              #?(:clj [clojure.java.io :as io]))
    #?(:clj (:import [java.io File])))


(def ^:const current-dir ".")
(def ^:const parent-dir "..")
(def ^:const directory-property "base.dir")
(def ^:const directory-env "ASAMI_BASE_DIR")

#?(:clj
   (defn get-directory
     ([name] (get-directory name true))
     ([name test?]
      (let [[root & path-elements] (string/split name #"/")]
        (when (or (= parent-dir root) (some #{current-dir parent-dir} path-elements))
          (throw (ex-info "Illegal path present in database name" {:database name})))
        (let [clean-path (cons root (remove empty? path-elements))
              clean-name (string/join File/separatorChar clean-path)
              base-dir (or (System/getProperty directory-property) (System/getenv directory-env))
              d (if base-dir
                  (io/file base-dir clean-name)
                  (io/file clean-name))]
          (when test?
            (if (.exists d)
              (when-not (.isDirectory d)
                (throw (ex-info (str "'" d "' already exists as a file") {:name name :path (.getAbsolutePath d)})))
              (when-not (.mkdirs d)
                (throw (ex-info (str "Unable to create directory '" clean-name "'") {:path (.getAbsolutePath d)})))))
          d)))))

(defn create-block-manager
  "Creates a block manager"
  [name manager-name block-size nr-blocks]
  #?(:clj
     (let [d (get-directory name)]
       (block-file/create-managed-block-file (.getPath (io/file d manager-name)) block-size nr-blocks))))

(defn named-storage
  "A common function for opening storage with a given name. Must be provided with a storage constructor and the name.
  The root id indicates an index root, and may be nil for an empty index.
  The block count refers to the count of blocks in the storage."
  [storage-constructor name root-id block-count]
  #?(:clj
     (storage-constructor name root-id block-count)))
