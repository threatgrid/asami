# Change Log

## [2.2.1] - 2021-10-16
### Fixed
- Accepting java.time.Instant objects on the `since` and `as-of` database functions.

## [2.2.0] - 2021-10-10
### Added
- `transact-async` has been added. On the JVM this will return a future that has not yet been completed. There is currently no difference between `transact` and `transact-async` for ClojureScript.

## Changed
- Transaction results now includes datoms that reflect the changes to the database, and not the changes requested of the database.
- Calling `delete-database` on an in-memory database URL will clear any open connections of data. If data is transacted into the connection, it will be reattached to the database registry as if it had been freshly opened.

## [2.1.3] - 2021-09-10
### Added
- OR expressions no longer require the same variable bindings for each sub-expression

## [2.1.2] - 2021-08-18
### Added
- Queries now test that projected variables in the `:find` clause appear in the `:where` clause.
- In memory graphs now how transaction IDs (not yet exposed in public APIs).

### Fixed
- User plans now using the correct return type, so complex queries don't cause errors while testing.
- Multiple bindings in the `:in` clause will now combine correctly during testing.

## [2.1.1] - 2021-06-28
### Added
- Added `asami.core/export-str`. This is a shortcut to convert a data export to a string, suitable for `clojure.core/spit`, since the default will write this as a LazySeq label.
- `asami.core/import-data` now accepts an `opts` argument, the same way that `edn` readers do. This is used when reading from `edn` text.

### Changed
- Single-end bindings on transitive constraints applied to memory graphs are now using the unbound form internally to gain log(n) complexity.
- Cleanups to internal code.

### Fixed
- In memory transitive attribute constraints have been fixed for the case of an unbound constraint for a given attribute.
- Boolean values saved to disk no longer create errors when reading back `false`.

## [2.1.0] - 2021-06-22
### Added
- Transactions include resource usage, so resources (files) can be cleaned up after failure.

### Fixed
- Recursive entities now correctly cut the recursion when extracting entities from the graph.
- Shutdown now releases connections. This truncates files to used space only.

### Changed
- Nodes now serialized using vector notation and without a redundant space. Previous node deserialization still supported.
- Serializing numbers is now smaller.
- Arrays and maps can now be serialized as values. This will show up in the API soon.
- Indexes now share file resources. This has removed 4 files.

## [2.0.6] - 2021-06-10
### Added
- A new CLI for loading data and executing queries. This builds natively with GraalVM.
- Exposing `now` and `instant?` from `asami.core`
- Serialization of arrays and maps. This will enable upcoming features.
- An `:id` attribute can now be used as a synonym for `:db/ident`. These values are not removed from entities, unlike the `:db` attributes.

### Fixed
- Aggregates correctly group even when selections are not sensible.

### Changed
- Integrating entity code fully into Asami. This is no longer imported from Zuko.
- Minor efficiency improvements to in-memory updates and reads.
- Booleans are now encoded into IDs and no longer serialized.

## [2.0.5] - 2021-05-27
### Changed
- Entity conversion to statements is no longer recursive on arrays. This allows for larger arrays.

## [2.0.4] - 2021-05-07
### Fixed
- Fixed missing functions on read-only trees

## [2.0.3] - 2021-05-06
### Fixed
- Fixed problem where the internal node IDs were not initializing from saved transactions.
- Imports now update the node ID generator to allocate IDs after imported data.

## [2.0.2] - 2021-04-28
### Fixed
- Removed multiple extensions for filenames.
- Fixed grouping for some aggregate queries.

## [2.0.1] - 2021-04-20
### Fixed
- Entity references to top-level entities no longer delete the referenced entity when the reference changes.

### Added
- Expanded handling of graph conversion for query objects, including getting the latest database from a connection.

### Changed
- Updated to Zuko 0.6.4.
- Top level entities now include `:tg/owns` edges to all sub structures that they own.

## [2.0.0] - 2021-04-08
### Changed
- Updated to Zuko 0.6.2, core.cache 1.0.207, and ClojureScript 1.10.844.

## [2.0.0-alpha9] - 2021-04-02
### Added
- Locking the transaction file during writes to prevent multiple processes from trying to modify it concurrently.

### Changed
- Opening files no longer allows variations on paths using . and ..

## [2.0.0-alpha8] - 2021-03-30
### Fixed
- Addressed concurrency bugs found in the JVM, for both memory-based and durable storage.

### Changed
- Updated to Zuko 0.6.0. This introduces new portable i/o operations.

## [2.0.0-alpha7] - 2021-03-20
### Changed
- Updated to Zuko 0.5.1. This allows arbitrary keytypes for entities.

## [2.0.0-alpha6] - 2021-03-19
### Changed
- Updated to Zuko 0.5.0. This means that entities without a temporary ID do not map their new IDs back to themselves in the `:tempids` of transactions.
- Zuko no longer brings in the unneeded Cheshire and JacksonXML dependencies.
- Cleaned up reflection in the durable layer, with a 35% speed improvement.

## [2.0.0-alpha5] - 2021-03-18
### Added
- `count-triple` implemented to scan index tree with reduced block access.

## [2.0.0-alpha4] - 2021-03-17
### Added
- Supporting lookup refs in transactions (thanks to @mk)
- Supporting transitive attributes for durable graphs.

### Fixed
- Fixed some transitive attribute operations that did not handle zero-steps correctly.

### Changed
- Updated to Zuko 0.4.6. This adds lookup refs to entities in transactions.

## [2.0.0-alpha3] - 2021-03-10
### Fixed
- Fixed bug that ignored :db/retract statements.

## [2.0.0-alpha2] - 2021-03-09
### Added
- Internal node type. This avoids the need for interning keywords as nodes.
- Added the `asami.Peer` class. This is very early access.

### Changed
- Updated to Zuko 0.4.4. This shifts the function whitelist into Zuko, and reduces the number of functions referenced in ClojureScript.
- Added functions for `and` and `or`.

### Fixed
- Functions from `clojure.string` can now be accessed in Clojure.

## [2.0.0-alpha] - 2021-03-05
### Added
- Durable storage provisioned on mapped files.
- Projection styles now work on aggregates
- `count`, `count-distinct` and `sample` can work on wildcards.

### Fixed
- `count` now de-duplicates, and `count-distinct` introduced.

## [1.2.15] - 2021-02-19
### Fixed
- Bugfix for multigraph entities

## [1.2.14] - 2021-02-18
### Changed
- Removed Clojurescript from the dependency tree of the generated artifacts.

## [1.2.13] - 2021-02-03
### Added
- Some Trace and Debug level logging for transactions and queries.

### Changed
- Moved to Zuko 0.4.0.

## [1.2.12] - 2021-01-19
### Added
- Bindings and Filters are now restricted by default. Introduced `asami.query/*override-restrictions*` flag to avoid restrictions.
- Can now filter by operations that are retrieved or calculated per row.
- Added internal API for Connections to expose their transaction ID.
- Added extra API schema in the Connection sources

## [1.2.11] - 2021-01-12
### Fixed
- Updated schema definition of `core/transact`. This only affected code with schema validation. 

## [1.2.10] - 2021-01-11
### Added
- New update-fn argument accepted in `asami.core/transact` to allow direct graph update operations.

## [1.2.9] - 2021-01-07
### Fixed
- Auto generated connection URIs were malformed, and have been fixed.

## [1.2.8] - 2020-12-14
### Changed
- Updated to Zuko 0.3.3 for performance improvement in loading entities.

## [1.2.7] - 2020-12-03
### Added
- Added support for variables to be used as functions in filters. Previously this was only possible in bindings.

## [1.2.6] - 2020-11-09
### Added
- Added `nested?` flag to the `entity` function.

## [1.2.5] - 2020-09-08
### Fixed
- Removed references to dead library code in tests.

## [1.2.4] - 2020-09-04
### Added
- Allowing naga.store/Storage to be used in a query.
- Added support for nil entries, via Zuko.

### Fixed
- Changing to Zuko 0.3.1 for bugfix.

## [1.2.3] - 2020-09-03
### Added
- Supporting empty arrays in entities (via Zuko update)

### Changed
- Change to internal APIs for improved transactions

## [1.2.2] - 2020-08-27
### Fixed
- Fixed use of macro as a value in CLJS

### Added
- `delete-database` function.

### Changed
- Refactored Connections and Databases to sit behind a protocol for abstraction.

## [1.2.1] - 2020-08-21
### Fixed
- Fixed problem that duplicated optional constraints during query planning.
- Fixed zero steps for transitive paths between nodes.

## [1.2.0] - 2020-08-19
### Added
- User planning can now be selected with query options. Add `:planner :user` to the end of the query arguments.
- `optional` operator, as per the [SPARQL OPTIONAL](https://www.w3.org/TR/sparql11-query/#optionals) operation.
- Added `show-plan` function to observe how a query will be performed.

### Changed
- Selecting variables as transitive attributes returns a path vector for that column.

### Fixed
- Fixed issue with filter arguments sometimes failing.
- Transitive querying. Now handles unbound path elements better, planning is improved, and zero-steps are properly handled.
- Tuple selector in the `:find` clause now work for single elements, and with empty results.
- Single graph element in the `:in` clause now works.


## 1.1.0 - 2020-08-05
### Added
- Attributes can be modified on entities by using the `'` annotation (see [Replacement Annotation doc](https://github.com/threatgrid/asami/wiki/Transactions#replacement-annotations) for details)
- Arrays in entities can be appended to by using the `+` annotation on attributes (see [Append Annotation doc](https://github.com/threatgrid/asami/wiki/Transactions#append-annotation) for details)
- Queries now accept `_` symbols as wildcards, and accept shortened constraints (not just triples)
- Introduce new `:find` clause syntaxes (see [Find Clause Description doc](https://github.com/threatgrid/asami/wiki/Querying#find-clause-description) for details)
- Created documentation for the API, data structures, querying, and creating transactions.

### Changed
- Entities now represent multi-arity attributes as a set of values. If more than one of an attribute is found, then a set of those values is returned in the entity. Similarly, providing a set will result in using the attribute multiple times in the graph.

### Fixed
- Entity arrays are now returned as vectors and not lists.


## 1.0.0 - 2020-07-29
### Added
- New Datomic-style API
- Implemented Datom object for transaction reporting
- Entity API provided via Zuko

## 0.4.10 - 2020-06-05
### Added
- Introduced Update Annotations

[Unreleased]: https://github.com/threatgrid/asami/compare/2.2.1...HEAD
[2.2.1]: https://github.com/threatgrid/asami/compare/2.2.0...2.2.1
[2.2.0]: https://github.com/threatgrid/asami/compare/2.1.3...2.2.0
[2.1.3]: https://github.com/threatgrid/asami/compare/2.1.2...2.1.3
[2.1.2]: https://github.com/threatgrid/asami/compare/2.1.1...2.1.2
[2.1.1]: https://github.com/threatgrid/asami/compare/2.1.0...2.1.1
[2.1.0]: https://github.com/threatgrid/asami/compare/2.0.6...2.1.0
[2.0.6]: https://github.com/threatgrid/asami/compare/2.0.5...2.0.6
[2.0.5]: https://github.com/threatgrid/asami/compare/2.0.4...2.0.5
[2.0.4]: https://github.com/threatgrid/asami/compare/2.0.3...2.0.4
[2.0.3]: https://github.com/threatgrid/asami/compare/2.0.2...2.0.3
[2.0.2]: https://github.com/threatgrid/asami/compare/2.0.1...2.0.2
[2.0.1]: https://github.com/threatgrid/asami/compare/2.0.0...2.0.1
[2.0.0]: https://github.com/threatgrid/asami/compare/2.0.0-alpha9...2.0.0
[2.0.0-alpha9]: https://github.com/threatgrid/asami/compare/2.0.0-alpha8...2.0.0-alpha9
[2.0.0-alpha8]: https://github.com/threatgrid/asami/compare/2.0.0-alpha7...2.0.0-alpha8
[2.0.0-alpha7]: https://github.com/threatgrid/asami/compare/2.0.0-alpha6...2.0.0-alpha7
[2.0.0-alpha6]: https://github.com/threatgrid/asami/compare/2.0.0-alpha5...2.0.0-alpha6
[2.0.0-alpha5]: https://github.com/threatgrid/asami/compare/2.0.0-alpha4...2.0.0-alpha5
[2.0.0-alpha4]: https://github.com/threatgrid/asami/compare/2.0.0-alpha3...2.0.0-alpha4
[2.0.0-alpha3]: https://github.com/threatgrid/asami/compare/2.0.0-alpha2...2.0.0-alpha3
[2.0.0-alpha2]: https://github.com/threatgrid/asami/compare/2.0.0-alpha...2.0.0-alpha2
[2.0.0-alpha]: https://github.com/threatgrid/asami/compare/1.2.14...2.0.0-alpha
[1.2.15]: https://github.com/threatgrid/asami/compare/1.2.14...1.2.15
[1.2.14]: https://github.com/threatgrid/asami/compare/1.2.13...1.2.14
[1.2.13]: https://github.com/threatgrid/asami/compare/1.2.12...1.2.13
[1.2.12]: https://github.com/threatgrid/asami/compare/1.2.11...1.2.12
[1.2.11]: https://github.com/threatgrid/asami/compare/1.2.10...1.2.11
[1.2.10]: https://github.com/threatgrid/asami/compare/1.2.9...1.2.10
[1.2.9]: https://github.com/threatgrid/asami/compare/1.2.8...1.2.9
[1.2.8]: https://github.com/threatgrid/asami/compare/1.2.7...1.2.8
[1.2.7]: https://github.com/threatgrid/asami/compare/1.2.6...1.2.7
[1.2.6]: https://github.com/threatgrid/asami/compare/1.2.5...1.2.6
[1.2.5]: https://github.com/threatgrid/asami/compare/1.2.4...1.2.5
[1.2.4]: https://github.com/threatgrid/asami/compare/1.2.3...1.2.4
[1.2.3]: https://github.com/threatgrid/asami/compare/1.2.2...1.2.3
[1.2.2]: https://github.com/threatgrid/asami/compare/1.2.1...1.2.2
[1.2.1]: https://github.com/threatgrid/asami/compare/1.2.0...1.2.1
[1.2.0]: https://github.com/threatgrid/asami/compare/1.1.0...1.2.0
