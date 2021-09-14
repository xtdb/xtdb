# 17 Classic Arbitrary Types

Date: 2021-09-14

## Status

Proposed

## Context

We would move towards c2's Arrow based data model
([ADR-0002](0002-data-model.md)).

Nippy (or arbitrary Java objects) will not be supported in XTDB by
default but this Arrow extension type can be enabled via a flag. These
columns will be Arrow varbinary to non-JVM users.

## Consequences
