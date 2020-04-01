# Changelog

## 20.04-1.8.1-alpha

See the [Github milestone](https://github.com/juxt/crux/milestone/7?closed=1) for the issues included in this release

## 20.03-1.8.0-alpha

See the [Github milestone](https://github.com/juxt/crux/milestone/6?closed=1) for the issues included in this release

## 20.03-1.7.1-alpha

See the [Github milestone](https://github.com/juxt/crux/milestone/5?closed=1) for the issues included in this release

## 20.02-1.7.0-alpha

### Changes

### Breaking changes
* [#555](https://github.com/juxt/crux/issues/555): Removes `submitted-tx-updated-entity?` and `submitted-tx-corrected-entity?` from the API, replacing them with a new function, `tx-committed?`.
* [#556](https://github.com/juxt/crux/issues/556): Removes `new-tx-log-context` and `tx-log` from the API, replacing them with `open-tx-log`.
* [#619](https://github.com/juxt/crux/issues/619): Standalone topology uses memdb (not persisted by default), removes RocksDB dependency from crux-core. See https://opencrux.com/docs#config-rocksdb for details about how to configure RocksDB
* [#496](https://github.com/juxt/crux/issues/496): Adding rocksdb metrics. The `kv-store` is now set as a module inside the topology vector.

### Bug fixes
* [#565](https://github.com/juxt/crux/pull/565): query predicate at zero join depth can now stop tuples from being returned
* [#507](https://github.com/juxt/crux/issues/507): ranges in rules revert to a predicate if neither argument is a logic var

### New Features
* [#466](https://github.com/juxt/crux/issues/466): Splitting `sync`'s various arities into `(sync node <timeout>)`, `(await-tx-time node tx-time <timeout>)` and `(await-tx node tx <timeout>)` (`(sync node tx-time <timeout>)` is deprecated and will be removed in a subsequent release).
* [#625](https://github.com/juxt/crux/issues/625): Metrics can be displayed to a prometheus server
* [#597](https://github.com/juxt/crux/issues/597): Metrics can be now analysed in cloudwatch
* [#495](https://github.com/juxt/crux/issues/495): Adding metrics to expose various indexer ingest metrics
* [#494](https://github.com/juxt/crux/issues/494): Adding metrics to expose metrics on how long queries are taking
* [#492](https://github.com/juxt/crux/issues/492): Adding metrics to expose local disk usage
* [#494](https://github.com/juxt/crux/issues/494): Adding metrics to expose metrics on how long queries are taking
* [#495](https://github.com/juxt/crux/issues/495): Adding metrics to expose various indexer ingest metrics
* [#568](https://github.com/juxt/crux/issues/568): You can now supply a vector of modules to `crux.node/topology`
* [#586](https://github.com/juxt/crux/issues/586): Common artifacts are now deployed on every release, check out the [releases](https://github.com/juxt/crux/releases) page on the repository and the Crux [dockerhub](https://hub.docker.com/u/cruxdockerhub) account.
* [#596](https://github.com/juxt/crux/issues/596): The HTTP server is now a module, to be included in the `:crux.node/topology` vector. See https://opencrux.com/docs#config-http for more details.
* [#597](https://github.com/juxt/crux/issues/597): Metrics can be now analysed in cloudwatch
* [#625](https://github.com/juxt/crux/issues/625): Metrics can be displayed to a prometheus server

## 20.01-1.6.2-alpha

### Changes
* [#524](https://github.com/juxt/crux/issues/524): Batch ingesting docs into the KV store
* [#520](https://github.com/juxt/crux/pull/520): New `entity` arity accepts a snapshot argument for >40% performance boost

### Bug fixes
* [#371](https://github.com/juxt/crux/issues/371): Documents in failed CaS operations now get evicted correctly
* [#434](https://github.com/juxt/crux/issues/434): Fix put/delete/CaS semantics when a transaction contains overlapping valid-time ranges for the same entity
* [#545](https://github.com/juxt/crux/issues/545): Fix greater than range predicate bug on empty db
* [#546](https://github.com/juxt/crux/issues/546): `crux.node/db` errors if provided a tx-time later than the latest completed tx, to preserve repeatability of queries.

## 19.12-1.6.1-alpha

### Bug fixes

* [#506](https://github.com/juxt/crux/issues/506): Fix lifetime of EntityAsOfIdx within put/delete, was being closed too soon.
* [#512](https://github.com/juxt/crux/issues/512): Fix race condition initialising id-hashing

## 19.12-1.6.0-alpha

### Breaking changes

These changes bump the index version to version 5 - a re-index of Crux nodes is required.

* [#428](https://github.com/juxt/crux/issues/428): Time ranges removed from 'evict' command, see [#PR438](https://github.com/juxt/crux/pull/438) for more details.
* [#441](https://github.com/juxt/crux/issues/441): Fix two transactions updating the same entity in the same millisecond always returning the earlier of the two values - requires index rebuild.
* [#326](https://github.com/juxt/crux/issues/326): Put/delete with start/end valid-time semantics made consistent

### New features
* [#363](https://github.com/juxt/crux/pull/363): Allow `full-results?` and other boolean flags in a vector-style query
* [#372](https://github.com/juxt/crux/issues/372): Add support for Java collection types with submitTx
* [#377](https://github.com/juxt/crux/issues/377): Can use 'cons' within query predicates
* [#414](https://github.com/juxt/crux/issues/414): Developer tool for query tracing
* [#430](https://github.com/juxt/crux/issues/430): Add LMDB configuration example to docs + tests
* [#457](https://github.com/juxt/crux/issues/457): Allowing nil to be returned from tx-fns

### Bug fixes

* [#362](https://github.com/juxt/crux/issues/362): Fixes 362 where hashes of small maps were dependent on order
* [#365](https://github.com/juxt/crux/issues/365): Replace usages of 'pr-str' with 'pr-edn-str' under crux.io
* [#367](https://github.com/juxt/crux/issues/367): Can query empty DB
* [#351](https://github.com/juxt/crux/issues/351): Do not merge placeholders into unary results
* [#368](https://github.com/juxt/crux/issues/368): Protect calls to modules when node is closed
* [#418](https://github.com/juxt/crux/issues/418): Adds exception when query with order-by doesn't return variable ordered on
* [#419](https://github.com/juxt/crux/issues/419): Fix specification for ':timeout' within queries.
* [#440](https://github.com/juxt/crux/issues/440): Fix return type of 'documents' in the API.
* [#453](https://github.com/juxt/crux/issues/453): Add nil check for queries in spec.
* [#454](https://github.com/juxt/crux/issues/454): Add fix for tx-log breaking in spec after an eviction
* [#482](https://github.com/juxt/crux/issues/482): Bringing README up to date.
* [#486](https://github.com/juxt/crux/issues/486): Race condition initialising hash implementation

## 19.09-1.5.0-alpha

### Changes

* [#340](https://github.com/juxt/crux/pull/340): *Breaking* Improve node configuration API, introduce topologies
* [#341](https://github.com/juxt/crux/issues/341): Various documentation improvements

### Bug fixes

* [#352](https://github.com/juxt/crux/issues/352): Fix Kotlin multhreaded node-start issues
* [#348](https://github.com/juxt/crux/issues/348): Increase range constraints var-frequency for join order

## 19.09-1.4.0-alpha

### Changes
* [#285](https://github.com/juxt/crux/issues/285): Various doc and tx spec enhancements
* [#287](https://github.com/juxt/crux/issues/287): Caching checking of specs for queries, 30% speed up for simple queries

### New features
* [PR #297](https://github.com/juxt/crux/pull/297): Support for PostgreSQL, MySQL, Oracle, SQLite, H2 via `crux-jdbc` (see subsequent commits on `master`)
* [PR #319](https://github.com/juxt/crux/pull/318): Kafka source and sink connectors
* [#176](https://github.com/juxt/crux/issues/176): Ingest API for some operations (without full node / indexes)
* [PR #320](https://github.com/juxt/crux/pull/320): *Experimental* support for transaction functions (disabled by default)

### Bug fixes
* [#314](https://github.com/juxt/crux/issues/314): NPE when submitting a query with empty args

## 19.07-1.3.0-alpha

### Changes

* [PR #300](https://github.com/juxt/crux/pull/300): *Breaking* replace all
  usage of the word "system" with "node" (including APIs/docs/examples) e.g. `start-standalone-system` is now `start-standalone-node`

### New features

* [PR #297](https://github.com/juxt/crux/pull/297): New `crux-jdbc` backend as an alternative to Kafka for clustered deployments

## 19.07-1.2.0-alpha

### Changes

* [PR #281](https://github.com/juxt/crux/pull/281): *Breaking* deprecate `juxt/crux` release, use `crux-core` and new modular deps
* [#288](https://github.com/juxt/crux/issues/288): Allow `crux-kafka` client configuration via `:kafka-properties-map`

### New features

* [PR #289](https://github.com/juxt/crux/issues/289): Merge WIP `crux-console` UI

## 19.07-1.1.1-alpha

### Changes

* [#266](https://github.com/juxt/crux/issues/266): `event-log-dir` is mandatory for standalone mode
* [PR #235](https://github.com/juxt/crux/pull/235): Add entity cache to improve large query performance

### Bug fixes

* [#268](https://github.com/juxt/crux/issues/268): `sync` timeout must be a duration
* [#272](https://github.com/juxt/crux/issues/272):  HTTP server can use `#crux/id` reader

## 19.06-1.1.0-alpha

### Changes

* [#208](https://github.com/juxt/crux/issues/208): *Breaking* Remove need for specifying ID in put and cas operations
* [#254](https://github.com/juxt/crux/issues/254): *Breaking* Move blocking `db` call to sync
* [PR #243](https://github.com/juxt/crux/pull/243): Refactoring: Split out api spec

### Bug fixes

* [#240](https://github.com/juxt/crux/issues/245): Fix using maps as ID
* [#241](https://github.com/juxt/crux/issues/241): Events checked against schema
* [#198](https://github.com/juxt/crux/issues/198): Fix docs nav links in safari
* [PR #206](https://github.com/juxt/crux/pull/206): Handle current max lag being unknown
* [#222](https://github.com/juxt/crux/issues/222): Fix race with eviction and caching

## 19.04-1.0.3-alpha

### New features

* [PR #174](https://github.com/juxt/crux/pull/174): Make `index` Reducible (thank you @mpenet)
* [#173](https://github.com/juxt/crux/issues/173): Include documents with queries results using `:full-results?`

### Changes

* [#202](https://github.com/juxt/crux/issues/202): Enforced use of `Date` instead of `Inst` within the transaction spec

### Bug fixes

* [#183](https://github.com/juxt/crux/issues/183): Fixes for JDK8 compilation
* [#180](https://github.com/juxt/crux/issues/180): Fix indexer replay of eviction transactions
* [#189](https://github.com/juxt/crux/issues/189): Fix evictions in standalone mode
* [#184](https://github.com/juxt/crux/issues/184): Improved testing for document eviction support

## (Changelog Template)

### Changes
* [PR #N](https://github.com/juxt/crux/pull/N): *Breaking* example
* [#N](https://github.com/juxt/crux/issues/N): issue example

### New features

### Bug fixes
