## CRUX Implementation Notes

See the [MVP Summary](mvp.md) for the background and motivation behind
CRUX.

The document [Internals](internals.md) (and the other docs in this
folder) discusses our ideas *before* we wrote the MVP, while this
document aims to explain how it actually works *after* we wrote the
MVP. As can be expected, some early ideas live on.

### Introduction

This document mainly discusses the query engine, as this is the core
of CRUX and where most its complexity lies. There are other parts of
CRUX, like the ingestion piece, the KV store implementations, and the
REST API, but they are more straight forward to understand and
requires less theoretical background.

### Indexes

### Query
