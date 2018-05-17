# Crux

[![CircleCI](https://circleci.com/gh/juxt/crux.svg?style=svg&circle-token=867b84b6d1b4dfff332773f771457349529aee8b)](https://circleci.com/gh/juxt/crux)

A Clojure graph database currently sitting on top of RocksDB or LMDB.

Motivation: a database with the following characteristics:

+ [Graph Query](docs/query.md)
+ [Additive Schema](docs/schema.md)
+ [Bitemporality](docs/bitemp.md)
+ [Data retention/excision strategies](docs/retention.md) (for GDPR)
+ [Global locking](docs/transactions.md)
+ Rapid data ingestion (using Kafka)
+ Scalable Query Engine using RocksDB

## Business Model

+ [Business Model](docs/business_model.md)
+ [Competitors](docs/competitors.md)

## Design Notes

Abstraction levels:

+ Db: high level db as a value (as-of)
+ Kv: Db implementation, exposes get and put, uses timestamps passed in as fn args
+ RocksDb: specific KV implementation

## References

+ https://www.datomic.com
+ https://github.com/fyrz/rocksdb-counter-microservice-sample
+ https://clojure.github.io/clojure-contrib/doc/datalog.html
+ https://github.com/tonsky/datascript
+ https://en.wikipedia.org/wiki/Jena_(framework)
+ https://arxiv.org/abs/1402.2237
