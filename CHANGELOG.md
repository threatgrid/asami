# Change Log

## [Unreleased]
### Changed


## [1.1.0] - 2020-08-05
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

[Unreleased]: https://github.com/threatgrid/asami/compare/1.1.0...HEAD
[1.1.0]: https://github.com/threatgrid/asami/compare/0.4.10...1.1.0
