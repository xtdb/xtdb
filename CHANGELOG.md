# Changelog

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
