# asami [![Build Status](https://travis-ci.org/threatgrid/asami.svg?branch=main)](https://travis-ci.org/threatgrid/asami) [![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-v2.0%20adopted-ff69b4.svg)](CODE_OF_CONDUCT.md)

A graph database, for Clojure and ClojureScript.

The latest version is :

[![Clojars Project](http://clojars.org/org.clojars.quoll/asami/latest-version.svg)](http://clojars.org/org.clojars.quoll/asami)

Asami is a _schemaless_ database, meaning that data may be inserted with no predefined schema. This flexibility has advantages and disadvantages. It is easier to load and evolve data over time without a schema. However, functionality like upsert and basic integrity checking is not available in the same way as with a graph with a predefined schema.

Asami also follows an _Open World Assumption_ model, in the same way that [RDF](http://www.w3.org/TR/rdf-primer) does. In practice, this has very little effect on the database, beyond what being schemaless provides.

If you are new to graph databases, then please read our [Introduction page](https://github.com/threatgrid/asami/wiki/2.-Introduction).

Asami has a query API that looks very similar to a simplified Datomic. More details are available in the [Query documentation](https://github.com/threatgrid/asami/wiki/6.-Querying).

## Features
There are several other graph databases available in the Clojure ecosystem, with each having their own focus. Asami is characterized by the following:
- Clojure and ClojureScript: Asami runs identically in both systems.
- Schema-less: Asami does not require a schema to insert data.
- Query planner: Queries are analyzed to find an efficient execution plan. This can be turned off.
- Analytics: Supports fast graph traversal operations, such as transitive closures, and can identify subgraphs.
- Integrated with Loom: Asami graphs are valid Loom graphs, via [Asami-Loom](https://github.com/threatgrid/asami-loom).
- Open World Assumption: Related to being schema-less, Asami borrows semantics from [RDF](http://www.w3.org/TR/rdf-primer) to lean towards an open world model.
- Pluggable Storage: Like Datomic, storage in Asami can be implemented in multiple ways. There are currently 2 in-memory graph systems, and durable storage available on the JVM.

## Usage
### Installing
Using Asami requires [Clojure](https://clojure.org/guides/getting_started) or [ClojureScript](https://clojurescript.org/guides/quick-start).

Asami can be made available to clojure by adding the following to a `deps.edn` file:
```clojure
{
  :deps {
    org.clojars.quoll/asami {:mvn/version "2.2.2"}
  }
}
```

This makes Asami available to a repl that is launched with the `clj` or `clojure` commands.

Alternatively, Asami can be added for the Leiningen build tool by adding this to the `:dependencies` section of the `project.clj` file:
```clojure
[org.clojars.quoll/asami "2.2.2"]
```

### Important Note for databases before 2.1.0
Asami 2.1.0 now uses fewer files to manage data. This makes it incompatible with previous versions. To port data from an older store to a new one, use the `asami.core/export-data` function on a database on the previous version of Asami, and `asami.core/import-data` to load the data into a new connection.

### Running
The [Asami API](https://github.com/threatgrid/asami/wiki/7.-Asami-API) tries to look a little like Datomic.

Once a repl has been configured for Asami, the following can be copy/pasted to test the API:
```clojure
(require '[asami.core :as d])

;; Create an in-memory database, named dbname
(def db-uri "asami:mem://dbname")
(d/create-database db-uri)

;; Create a connection to the database
(def conn (d/connect db-uri))

;; Data can be loaded into a database either as objects, or "add" statements:
(def first-movies [{:movie/title "Explorers"
                    :movie/genre "adventure/comedy/family"
                    :movie/release-year 1985}
                   {:movie/title "Demolition Man"
                    :movie/genre "action/sci-fi/thriller"
                    :movie/release-year 1993}
                   {:movie/title "Johnny Mnemonic"
                    :movie/genre "cyber-punk/action"
                    :movie/release-year 1995}
                   {:movie/title "Toy Story"
                    :movie/genre "animation/adventure"
                    :movie/release-year 1995}])

@(d/transact conn {:tx-data first-movies})
```
The [`transact`](https://github.com/threatgrid/asami/wiki/7.-Asami-API#transact) operation returns an object that can be _dereferenced_ (via `clojure.core/deref` or the `@` macro) to provide information about the state of the database before and after the transaction. (A _future_ in Clojure, or a _delay_ in ClojureScript). Note that the transaction data can be provided as the `:tx-data` in a map object if other parameters are to be provided, or just as a raw sequence without the wrapping map.

For more information about loading data and executing `transact` see the [Transactions documentation](https://github.com/threatgrid/asami/wiki/4.-Transactions).

With the data loaded, a database value can be retrieved from the database and then queried.

**NB:** The `transact` operation will be executed asynchronously on the JVM. Retrieving a database immediately after executing a `transact` will not retrieve the latest database. If the updated database is needed, then perform the `deref` operation as shown above, since this will wait until the operation is complete.

```clojure
(def db (d/db conn))

(d/q '[:find ?movie-title
       :where [?m :movie/title ?movie-title]] db)
```
This returns a sequence of results, with each result being a sequence of the selected vars in the `:find` clause (just `?movie-title` in this case):
```
(["Explorers"]
 ["Demolition Man"]
 ["Johnny Mnemonic"]
 ["Toy Story"])
```
A more complex query could be to get the title, year and genre for all movies after 1990:
```clojure
(d/q '[:find ?title ?year ?genre
       :where [?m :movie/title ?title]
              [?m :movie/release-year ?year]
              [?m :movie/genre ?genre]
              [(> ?year 1990)]] db)
```
Entities found in a query can be extracted back out as objects using the [`entity`](https://github.com/threatgrid/asami/wiki/7.-Asami-API#entity) function. For instance, the following is a repl session that looks up the movies released in 1995, and then gets the associated entities:
```clojure
;; find the entity IDs. This variation in the :find clause asks for a list of just the ?m variable
=> (d/q '[:find [?m ...] :where [?m :movie/release-year 1995]] db)
(:tg/node-10327 :tg/node-10326)

;; get a single entity
=> (d/entity db :tg/node-10327)
#:movie{:title "Toy Story",
        :genre "animation/adventure",
        :release-year 1995}

;; get all the entities from the query
=> (map #(d/entity db %)
        (d/q '[:find [?m ...] :where [?m :movie/release-year 1995]] db))
(#:movie{:title "Toy Story",
         :genre "animation/adventure",
         :release-year 1995}
 #:movie{:title "Johnny Mnemonic",
         :genre "cyber-punk/action",
         :release-year 1995})
```
See the [Query Documentation](https://github.com/threatgrid/asami/wiki/6.-Querying) for more information on querying.

Refer to the [Entity Structure documentation](https://github.com/threatgrid/asami/wiki/5.-Entity-Structure) to understand how entities are stored and how to construct queries for them.

### Local Storage
The above code uses an in-memory database, specified with a URL of the form `asami:mem://dbname`. Creating a database on disk is done the same way, but with the URL scheme changed to `asami:local://dbname`. This would create a database in the `dbname` directory. Local databases do not use keywords as entity IDs, as keywords use up memory, and a local database could be gigabytes in size. Instead, these are `InternalNode` objects. These can be created with `asami.graph/new-node`, or by using the readers in `asami.graph`. For instance, if the above code were all done with a local graph instead of a memory graph:
```clojure
=> (d/q '[:find [?m ...] :where [?m :movie/release-year 1995]] db)
(#a/n "3" #a/n "4")

;; get a single entity
=> (require '[asami.graph :as graph])
=> (d/entity db (graph/new-node 4))
#:movie{:title "Toy Story", :genre "animation/adventure/comedy", :release-year 1995}

;; nodes can also be read from a string, with the appropriate reader
=> (set! *data-readers* graph/node-reader)
=> (d/entity db #a/n "4")
#:movie{:title "Toy Story", :genre "animation/adventure/comedy", :release-year 1995}
```

### Updates
The _Open World Assumption_ allows each attribute to be multi-arity. In a _Closed World_ database an object may be loaded to replace those attributes that can only appear once. To do the same thing with Asami, annotate the attributes to be replaced with a quote character at the end of the attribute name. 
```clojure
=> (def toy-story (d/q '[:find ?ts . :where [?ts :movie/title "Toy Story"]] db))
=> (d/transact conn [{:db/id toy-story :movie/genre' "animation/adventure/comedy"}])
=> (d/entity (d/db conn) toy-story)
#:movie{:title "Toy Story",
        :genre "animation/adventure/comedy",
        :release-year 1995}
```
Addressing nodes by their internal ID can be cumbersome. They can also be addressed by a `:db/ident` field if one is provided.
```clojure
(def tx (d/transact conn [{:db/ident "sense"
                           :movie/title "Sense and Sensibility"
                           :movie/genre "drama/romance"
                           :movie/release-year 1996}]))

;; ask the transaction for the node ID, instead of querying
(def sense (get (:tempids @tx) "sense"))
(d/entity (d/db conn) sense)
```
This returns the new movie. The `:db/ident` attribute does not appear in the entity:
```clojure
#:movie{:title "Sense and Sensibility", :genre "drama/romance", :release-year 1996}
```
However, all of the attributes are still present in the graph:
```clojure
=> (d/q '[:find ?a ?v :in $ ?s :where [?s ?a ?v]] (d/db conn) sense)
([:db/ident "sense"] [:movie/title "Sense and Sensibility"] [:movie/genre "drama/romance"] [:movie/release-year 1996])
```
The release year of this movie is incorrectly set to the release in the USA, and not the initial release. That can be updated using the `:db/ident` field:
```clojure
=> (d/transact conn [{:db/ident "sense" :movie/release-year' 1995}])
=> (d/entity (d/db conn) sense)
#:movie{:title "Sense and Sensibility", :genre "drama/romance", :release-year 1995}
```
More details are provided in [Entity Updates](https://github.com/threatgrid/asami/wiki/4.-Transactions#entity-updates).

## Analytics
Asami also has some support for graph analytics. These all operate on the _graph_ part of a database value, which can be retrieved with the `asami.core/graph` function.

**NB:** `local` graphs on disk are not yet supported. These will be available soon.

Start by populating a graph with the cast of ["The Flintstones"](https://www.imdb.com/title/tt0053502/). So that we can refer to entities after they have been created, we can provide them with temporary ID values. These are just negative numbers, and can be used elsewhere in the transaction to refer to the same entity. We will also avoid the `:tx-data` wrapper in the transaction:
```clojure
(require '[asami.core :as d])
(require '[asami.analytics :as aa])

(def db-uri "asami:mem://data")
(d/create-database db-uri)
(def conn (d/connect db-uri))

(def data [{:db/id -1 :name "Fred"}
           {:db/id -2 :name "Wilma"}
           {:db/id -3 :name "Pebbles"}
           {:db/id -4 :name "Dino" :species "Dinosaur"}
           {:db/id -5 :name "Barney"}
           {:db/id -6 :name "Betty"}
           {:db/id -7 :name "Bamm-Bamm"}
           [:db/add -1 :spouse -2]
           [:db/add -2 :spouse -1]
           [:db/add -1 :child -3]
           [:db/add -2 :child -3]
           [:db/add -1 :pet -4]
           [:db/add -5 :spouse -6]
           [:db/add -6 :spouse -5]
           [:db/add -5 :child -7]
           [:db/add -6 :child -7]])

(d/transact conn data)
```
Fred, Wilma, Pebbles, and Dino are all connected in a subgraph. Barney, Betty and Bamm-Bamm are connected in a separate subgraph.

Let's find the subgraph from Fred:
```clojure
(def db (d/db conn))
(def graph (d/graph db))
(def fred (d/q '[:find ?e . :where [?e :name "Fred"]] db))

(aa/subgraph-from-node graph fred)
```
This returns the _nodes_ in the graph, but not the scalar values. For instance:
```clojure
#{:tg/node-10330 :tg/node-10329 :tg/node-10331 :tg/node-10332}
```
These nodes can be used as the input to a query to get their names:
```clojure
=> (d/q '[:find [?name ...] :in $ [?n ...] :where [?n :name ?name]]
        db
        (aa/subgraph-from-node graph fred))
("Fred" "Pebbles" "Dino" "Wilma")
```

We can also get all the subgraphs:
```clojure
=> (count (aa/subgraphs graph))
2

;; execute the same query for each subgraph
=> (map (partial d/q '[:find [?name ...] :where [?e :name ?name]])
        (aa/subgraphs graph))
(("Fred" "Wilma" "Pebbles" "Dino") ("Barney" "Betty" "Bamm-Bamm"))
```

#### Transitive Queries
Asami supports transitive properties in queries. A property (or attribute) is treated as transitive if it is followed by a `+` or a `*` character.
```clojure
(d/q '[:find ?friend-of-a-friend
       :where [?person :name "Fred"]
              [?person :friend+ ?foaf]
              [?foaf :name ?friend-of-a-friend]]
     db)
```
This will find all friends, and friends of friends for Fred.

### Loom
Asami also implements [Loom](https://github.com/aysylu/loom) via the [Asami-Loom](https://github.com/threatgrid/asami-loom) package.
Include the following dependency for your project:
```
[org.clojars.quoll/asami-loom "0.2.0"]
```

Graphs can now be analyzed with Loom functions.

If functions are provided to Loom, then they can be used to provide labels for creating a visual graph. The following creates some simple queries to get the labels for edges and nodes:
```clojure
(require '[asami-loom.index])
(require '[asami-loom.label])
(require '[loom.io])

(defn edge-label [g s d]
  (str (d/q '[:find ?e . :in $ ?a ?b :where (or [?a ?e ?b] [?b ?e ?a])] g s d)))
  
(defn node-label [g n]
  (or (d/q '[:find ?name . :where [?n :name ?name]] g n) "-"))

;; create a PDF of the graph
(loom-io/view (graph db) :fmt :pdg :alg :sfpd :edge-label edge-label :node-label node-label)
```

## Command Line Tool
A command line tool is available to load data into an Asami graph and query it. This requires [GraalVM CE 21.1.0](https://www.graalvm.org/) or later, and the [`native-image`](https://www.graalvm.org/reference-manual/native-image/#install-native-image) executable.
Leiningen needs to see GraalVM on the classpath first, so if there are problems with building, check to see if this is the case.

To build from sources:

```bash
lein with-profile native uberjar
lein with-profile native native
```

This will create a binary called `asami` in the `target` directory. Execute with the `-?` flag for help:

```
$ ./target/asami -?
Usage: asami URL [-f filename] [-e query] [--help | -?]

-? | --help: This help
URL: the URL of the database to use. Must start with asami:mem://, asami:multi:// or asami:local://
-f filename: loads the filename into the database. A filename of "-" will use stdin.
             Data defaults to EDN. Filenames ending in .json are treated as JSON.
-e query: executes a query. "-" (the default) will read from stdin instead of a command line argument.
          Multiple queries can be specified as edn (vector of query vectors) or ; separated.

Available EDN readers:
  internal nodes -  #a/n "node-id"
  regex          -  #a/r "[Tt]his is a (regex|regular expression)"
```

#### Example:
Loading a json file, and querying for keys (attributes) that are strings with spaces in them:

```bash
asami asami:mem://tmp -f data.json -e ':find ?a :where [?e ?a ?v][(string? ?a)][(re-find #a/r " " ?a)]'
```

The command will also work on `local` stores, which means that they can be loaded once and then queried multiple times.

## License

Copyright © 2016-2021 Cisco

Portions of src/asami/cache.cljc are Copyright © Rich Hickey

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
