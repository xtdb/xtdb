# Changelog

## 19.09-1.5.0-alpha

### Changes

* [#340](https://github.com/juxt/crux/pull/340): Improve node configuration API, introduce topologies *Breaking*
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
