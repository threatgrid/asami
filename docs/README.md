# Introduction to Asami

Asami is a simple in-memory graph database. It was initially built as an in-memory graph for Naga, but now exists in it's own project. The principal API is through the [Storage](https://github.com/threatgrid/naga-store/blob/master/src/naga/store.cljc#L8) protocol defined in [naga-store](https://github.com/threatgrid/naga-store).

Asami does not (yet) have a rich set of features, but attempts to keep the major components modular, and relatively simple.

## Layout

### core
The `asami.core` namespace is where the main APIs for interacting with Asami are found. For now, these are:
* `MemoryStore`: A record type that implements the `naga.store/Storage` protocol.
* `empty-store`: An instance of `MemoryStore`, ready to insert data.
* `empty-multi-store`: An instance of `MemoryStore` that implements a multigraph (multiple edges are recorded), ready to insert data.
* `q`: A query function, with the same syntax as the Datomic `q` function, but fewer features.

### query
The `asami.query` namespace is where the query implementation appears. This includes query planning, and execution. Planning and execution are separate operations, despite being related, and so this namespace is expected to split soon.

### graph
The `asami.graph` namespace defines the `Graph` protocol. This is a simple triple-only interface for abstracting storage. The `graph-diff` function is unused for most applications and may be considered optional. It identifies entities whose attributes have changed from one graph to another.

I am hoping to finish a durable storage implementation, based on a design that [Mulgara](https://github.com/quoll/mulgara) was working towards.

### index
The principle implementation of `asami.graph`. This uses indices of nested maps, terminating at sets.

### multi-graph
Alternative multi-graph implementation of `asami.graph`. Multi-graphs are the same as graphs, but record how many times an edge has been inserted.

Indices of Nested maps, terminating at a statement count. This should be generalized to a map that contains metadata for the statement (and include a count).

### asami-util
Central utility namespace. At this point, it contains `c-eval` and `fn-for`.

`c-eval` was used as a portable (Clojure and ClojureScript) `eval` function for bindings and filters in queries. However, some self-hosted ClojureScript environments have difficulty with evaluation, so another approach is now being used.

`fn-for` is a portable function for finding functions by symbol. If no namespace is given, then it defaults to looking in `clojure.core`.

## Talks

* Presentation at ClojureD, February 23, 2019. [Animated slides](https://threatgrid.github.io/asami/talks/Graphs/index.html). [PDF](https://threatgrid.github.io/asami/talks/Graphs.pdf) (includes speaker notes).

