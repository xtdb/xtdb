# 14 Classic lucene

Date: 2021-09-14

## Status

Proposed

## Context

Support classic-like lucene functionality in Datalog

A c2-native solution would be to have secondary indexes
participate in finish chunk, and maybe also a collective LSM-style
merge process. Temporal could maybe also use this capability if it
existed.

## Consequences
