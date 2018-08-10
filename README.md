# Crux

[![CircleCI](https://circleci.com/gh/juxt/crux.svg?style=svg&circle-token=867b84b6d1b4dfff332773f771457349529aee8b)](https://circleci.com/gh/juxt/crux)

A Clojure graph database currently sitting on top of RocksDB or LMDB, and Kafka.

## MVP

+ [MVP Summary](docs/mvp.adoc)
+ [Crux Implementation Notes](docs/implementation.adoc)

## Phase 2

+ [Phase 2](docs/phase_2.adoc)

## Background

Motivation: a database with the following characteristics:

+ [Graph Query](docs/query.adoc)
+ [Additive Schema](docs/schema.adoc)
+ [Bitemporality](docs/bitemp.adoc)
+ [Data retention/excision strategies](docs/retention.adoc) (for GDPR)
+ [Global locking](docs/transactions.adoc)
+ Rapid data ingestion (using Kafka)
+ Scalable Query Engine using RocksDB

The documents above contain many references.

## Business Model

+ [Business Model](docs/business_model.adoc)
+ [Competitors](docs/competitors.adoc)

## Building

``` sh
lein uberjar
java -jar target/crux-*-standalone.jar --help
```

## Developing

Start a REPL. To run the system with embedded Kafka and ZK:

``` clojure
(dev)
(start)
```

This will store data under `dev-storage` in the checkout directory.
