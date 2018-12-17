# Crux

[![CircleCI](https://circleci.com/gh/juxt/crux.svg?style=svg&circle-token=867b84b6d1b4dfff332773f771457349529aee8b)](https://circleci.com/gh/juxt/crux)

Crux is an unbundled, distributed, bitemporal graph database, currently sitting on top of RocksDB or LMDB, and Kafka.

## Manual

Please visit and read our [manual](https://juxt.pro/crux/docs/index.html).

Until Crux is publicly released, you will need credentials; userame `crux`, password `advisor`.

Please give us your feedback, and enjoy Crux.

## Roadmap

+ [Roadmap](docs/roadmap.adoc)

## Building

``` sh
lein uberjar
java -jar target/crux-*-standalone.jar --hel1p
```

## Developing

Start a REPL. To run the system with embedded Kafka and ZK:

``` clojure
(dev)
(start)
```

This will store data under `dev-storage` in the checkout directory.
