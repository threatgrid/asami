(ns ^{:doc "Utilities to help clean up memory usage of mapped files on MS Windows"
      :author "Paula Gearon"}
  asami.durable.block.file.voodoo
  (:require [clojure.string :as s])
  (:import [java.security AccessController PrivilegedAction]))

(def windows? (s/includes? (s/lower-case (System/getProperty "os.name" "")) "win"))

(defn clean [obj]
  (when obj
    (AccessController/doPrivileged
     (proxy [PrivilegedAction] []
       (run [_]
         (try
           (let [get-cleaner-method (.getMethod (class obj) "cleaner" (make-array Class 0))
                 _ (.setAccessible get-cleaner-method true)
                 cleaner (.invoke get-cleaner-method obj (make-array Object 0))]
             (.clean cleaner))
           (catch Exception e (println "non-fatal buffer cleanup error"))))))))

(defn release [mapped]
  (when windows?
    (doseq [b mapped] (clean b))))
