# Changelog

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
