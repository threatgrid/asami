(ns asami.test-core-query
  "Tests the public query functionality"
  (:require [naga.store :refer [new-node assert-data]]
            [asami.core :refer [empty-store q]]
            [schema.core :as s]
            #?(:clj  [clojure.test :refer [is use-fixtures testing]]
               :cljs [clojure.test :refer-macros [is run-tests use-fixtures testing]])
            #?(:clj  [schema.test :as st :refer [deftest]]
               :cljs [schema.test :as st :refer-macros [deftest]]))
  #?(:clj (:import [clojure.lang ExceptionInfo])))

(defn nn [] (new-node empty-store))

(def pmc (nn))
(def gh (nn))
(def r1 (nn))
(def r2 (nn))
(def r3 (nn))
(def r4 (nn))

(def data [[pmc :artist/name "Paul McCartney"]
           [gh :artist/name "George Harrison"]
           [r1 :release/artists pmc]
           [r1 :release/name "My Sweet Lord"]
           [r2 :release/artists gh]
           [r2 :release/name "Electronic Sound"]
           [r3 :release/artists gh]
           [r3 :release/name "Give Me Love (Give Me Peace on Earth)"]
           [r4 :release/artists gh]
           [r4 :release/name "All Things Must Pass"]])

(def store (-> empty-store
               (assert-data data)))

(deftest test-simple-query
  (let [results (q '[:find ?e ?a ?v
                     :where [?e ?a ?v]] store)]
    (is (= (set data) (set results)))))

(deftest test-join-query
  (let [results (q '[:find ?name
                     :where [?r :release/name "My Sweet Lord"]
                            [?r :release/artists ?a]
                            [?a :artist/name ?name]] store)]
    (is (= [["Paul McCartney"]] results))))

(deftest test-join-multi-query
  (let [results (q '[:find ?rname
                     :where [?a :artist/name "George Harrison"]
                            [?r :release/artists ?a]
                            [?r :release/name ?rname]] store)]
    (is (= #{["Electronic Sound"]
             ["Give Me Love (Give Me Peace on Earth)"]
             ["All Things Must Pass"]} (set results)))))

(deftest test-bindings-query
  (let [results (q '[:find ?release-name
                     :in $ [?artist-name ...]
                     :where [?artist :artist/name ?artist-name]
                     [?release :release/artists ?artist]
                     [?release :release/name ?release-name]]
                   store ["Paul McCartney" "George Harrison"])]
    (is (= #{["My Sweet Lord"] 
             ["Electronic Sound"]
             ["Give Me Love (Give Me Peace on Earth)"] 
             ["All Things Must Pass"]}
           (set results)))))

(deftest test-query-error
  (is (thrown-with-msg? ExceptionInfo #"Missing ':find' clause"
                        (q '[:select ?e ?a ?v
                             :where [?e ?a ?v]] store)))
  (is (thrown-with-msg? ExceptionInfo #"Missing ':where' clause"
                        (q '[:find ?e ?a ?v
                             :given [?e ?a ?v]] store)))
  (is (thrown-with-msg? ExceptionInfo #"Unknown clauses: "
                        (q '[:select ?e ?a ?v
                             :having [(?v > 5)]
                             :where [?e ?a ?v]] store))))
