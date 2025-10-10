---
title: Mission
---

At [JUXT](https://juxt.pro/), the company behind XTDB, our mission is **to simplify how the world makes software**.

We want XTDB to reduce the time and money required for organizations to build and maintain [**safe**](https://www.juxt.pro/blog/kent-beck-podcast/) systems of record and their associated APIs.

## Time Matters

Time, and the passage thereof, is a factor in so many requirements and use-cases. 

It's hard to get right/fast, hard to retro-fit, and full of boilerplate.
Data doesn't always arrive in the right order, or indeed promptly, and often requires later corrections.

We believe existing database technologies (PostgreSQL, SQL Server, MongoDB etc.) leave these problems for their users to work around and solve ad-hoc in their application code.

> Any sufficiently complicated data system contains an ad-hoc, informally-specified, bug-ridden, slow implementation of half of a bitemporal database.
> 
> -- _"Henderson's Tenth Law" (with apologies to [Greenspun](https://en.wikipedia.org/wiki/Greenspun%27s_tenth_rule))_

We believe that DIY versioning within databases should be a thing of the past - preserving historical data should not have to be an explicit process which requires conscious design effort and careful development work.

## The Problems

In this era of rapidly evolving regulatory requirements, we see an increasing number of organizations across all industries feeling frustrated by the friction with [update-in-place](https://www.youtube.com/watch?v=JxMz-tyicgo) databases that don't support basic auditing requirements.

From GDPR to HIPAA to MiFID II, all kinds of regulatory requirements justify a fundamental level of accuracy and accountability in how your software stack stores and represents key information.
The implications extend across the entire transactional to analytical data lifecycle.

This lack of accurate time-based versioning has four main consequences that plague applications working with regulated data:

1. **Inconsistent reporting over historical data** - accurate reporting requires some notion of "time travel" queries, but without a stable *bitemporal* basis enforced by the database ad hoc solutions will inevitably resort to copying snapshots of entire data sets in a search for consistency.
2. **Inadequate auditing & compliance** - audit tables and triggers and change data capture are all workarounds for the absence of basic audit facilities within a database.
3. **Challenging data integration & evolution** - strict schema definitions, update-in-place mutations, and poor support for semi-structured data are all impediments to using relational databases as integration points.
4. **Difficulty of managing SQL with an ad-hoc bitemporal schema** - composing SQL is unnecessarily hard and is a common source of developer friction.
   Bitemporal versioning makes it harder still.
   
## The Solution

XTDB, therefore, is an immutable, time-travel database designed for building complex applications, supporting developers with features that are essential for meeting common regulatory compliance requirements.

It is built to help solve the hard problems of: 

- accurate record keeping (with full ACID transaction support), 
- time-travel queries (indeed, XT stands for "across time"), and
- data evolution.

XTDB achieves this by its ubiquitous ['bitemporal'](https://en.wikipedia.org/wiki/Bitemporal_modeling) time-based versioning of records.
It records the history of all changes (particularly UPDATEs and DELETEs, which are normally destructive operations), and by restricting the scope of concurrent database usage, to retain an auditable, linear sequence of all changes.

Beyond the obvious auditing and debugging benefits of retaining change data and prior database states, XTDB's history-preserving capability presents a robust & stable source of truth - a *'basis'* - within a wider IT architecture that is unlike anything that most databases can offer.
