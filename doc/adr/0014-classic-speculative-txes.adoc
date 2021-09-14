# 14 Classic-like Speculative Transactions

Date: 2021-09-14

## Status

Proposed

## Context

The easiest way to do this is c2 is to take defensive (or
copy-on-write) copies of the live roots in the watermark (a watermark
is kind of like a db in c2). This is a bit slow, but fundamentally
doable. Note that all data won't be needed to be copied, just the
current slice and the current in-memory temporal index.

## Consequences
