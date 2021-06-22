(ns ^{:doc "A common namespace for utility functions for storage. "
      :author "Paula Gearon"}
    asami.durable.common-utils
    (:require #?(:clj [asami.durable.block.file.block-file :as block-file])
              #?(:clj [clojure.java.io :as io])))


(defn create-block-manager
  "Creates a block manager"
  [name manager-name block-size nr-blocks]
  #?(:clj
     (block-file/create-managed-block-file (.getPath (io/file name manager-name)) block-size nr-blocks)))

(defn named-storage
  "A common function for opening storage with a given name. Must be provided with a storage constructor and the name.
  The root id indicates an index root, and may be nil for an empty index.
  The block count refers to the count of blocks in the storage."
  [storage-constructor name root-id block-count]
  #?(:clj
     (let [d (io/file name)]
       (if (.exists d)
         (when-not (.isDirectory d)
           (throw (ex-info (str "'" name "' already exists as a file") {:path (.getAbsolutePath d)})))
         (when-not (.mkdir d)
           (throw (ex-info (str "Unable to create directory '" name "'") {:path (.getAbsolutePath d)}))))
       (storage-constructor name root-id block-count))))
