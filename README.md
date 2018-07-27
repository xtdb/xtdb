# CRUX

[![CircleCI](https://circleci.com/gh/juxt/crux.svg?style=svg&circle-token=867b84b6d1b4dfff332773f771457349529aee8b)](https://circleci.com/gh/juxt/crux)

A Clojure graph database currently sitting on top of RocksDB or LMDB, and Kafka.

## MVP

+ [MVP Summary](docs/mvp.md)
+ [CRUX Implementation Notes](docs/implementation.md)

## Background

Motivation: a database with the following characteristics:

+ [Graph Query](docs/query.md)
+ [Additive Schema](docs/schema.md)
+ [Bitemporality](docs/bitemp.md)
+ [Data retention/excision strategies](docs/retention.md) (for GDPR)
+ [Global locking](docs/transactions.md)
+ Rapid data ingestion (using Kafka)
+ Scalable Query Engine using RocksDB

The documents above contain many references.

## Business Model

+ [Business Model](docs/business_model.md)
+ [Competitors](docs/competitors.md)
