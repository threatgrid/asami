# Change Log

## [Unreleased]
### Added
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

[Unreleased]: https://github.com/threatgrid/asami/compare/1.2.11...HEAD
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
