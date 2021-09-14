# 21 Classic Tx Ops

Date: 2021-09-14

## Status

Proposed

## Context

#### match/cas

Can be implemented as a scan of the same columns. For consistency, its
easiest to write the expected doc to Arrow as well and just compare
the rows.

## Consequences

Users will have to migrate their usage of transaction functions to
integrity checks.
