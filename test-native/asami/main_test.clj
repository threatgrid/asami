(ns asami.main-test
  (:require [asami.main :as native]
            [asami.core :as asami]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.test :as t])
  (:import (java.io File)
           (java.io PushbackReader)))

(t/deftest process-args-test
  (let [url "asami:"
        query "[:find ?x :where [?x ?y ?z]]"
        filename "foo.edn"]
    (t/is (= (native/process-args [])
             {:interactive? false
              :query "-"}))

    (t/is (= {:interactive? true
              :query "-"}
             (native/process-args ["--interactive"])))

    (t/is (= {:help true
              :interactive? false
              :query "-"}
             (native/process-args ["-?"])))

    (t/is (= {:help true
              :interactive? false
              :query "-"}
             (native/process-args ["--help"])))

    (t/is (= {:interactive? false
              :query "-"
              :url url}
             (native/process-args [url])))

    (t/is (= {:interactive? false
              :query query
              :url url}
             (native/process-args [url "-q" query])
             (native/process-args [url "-q" query])))
    
    (t/is (= {:file "foo.edn"
              :interactive? false
              :query query
              :url url}
             (native/process-args [url "-e" query "-f" filename])))))

(defmacro repl-out-str
  "Macro to help test asami.main/repl."
  [& args]
  `(with-out-str (native/repl ~@args)))

(t/deftest repl-test
  (let [conn (asami/connect (str "asami:mem://" (gensym)))
        tx-triples [["alice" :knows "bob"]
                    ["bob" :knows "alice"]]
        _ (deref (asami/transact conn {:tx-triples tx-triples}))
        db (asami/db conn)]
    (t/testing "one map query"
      (let [query "{:find [?x ?y] :where [[?x :knows ?y]]}"
            result (edn/read-string (repl-out-str (native/derive-stream {:query query}) db nil))
            result-set (into #{} result)]
        (t/is (= #{["alice" "bob"] ["bob" "alice"]}
                 result-set))))

    (t/testing "one vector query"
      (let [query "[:find ?x ?y :where [?x :knows ?y]]"
            result (edn/read-string (repl-out-str (native/derive-stream {:query query}) (asami/db conn) nil))
            result-set (into #{} result)]
        (t/is (= #{["alice" "bob"] ["bob" "alice"]}
                 result-set))))

    (t/testing "multiple queries"
      (let [query "{:find [?x] :where [[\"alice\" :knows ?x]]}
                   {:find [?x] :where [[\"bob\" :knows ?x]]}"
            result (repl-out-str (native/derive-stream {:query query}) (asami/db conn) nil)
            stream (PushbackReader. (io/reader (.getBytes result)))]
        (t/is (= #{[["alice"]] [["bob"]]} (conj #{} (edn/read stream) (edn/read stream))))))

    (let [query "{:find [?x ?y] :where [[?x :knows ?y]]"
          stream (native/derive-stream {:query query})
          result (repl-out-str stream (asami/db conn) nil)]
      (t/is (= result "Error: EOF while reading\n")))))

