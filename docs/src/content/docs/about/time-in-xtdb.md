---
title: Time in XTDB
---

Time, and the passage thereof, is a factor in so many requirements and use-cases. 

It's hard to get right/fast, hard to retro-fit, and full of boilerplate.
Data doesn't always arrive in the right order, or indeed promptly, and often requires later corrections.

We believe existing database technologies (PostgreSQL, SQL Server, MongoDB etc.) leave these problems for their users to work around and solve ad-hoc in their application code.

> Any sufficiently complicated data system contains an ad-hoc, informally-specified, bug-ridden, slow implementation of half of a bitemporal database.
> 
> -- _"Henderson's Tenth Law" (with apologies to [Greenspun](https://en.wikipedia.org/wiki/Greenspun%27s_tenth_rule))_

XTDB is built to eliminate the incidental complexities that are normally associated with handling time in a database.

XTDB makes time [simple](https://www.youtube.com/watch?v=SxdOUGdseq4).

## Bitemporality - 'two times'

Traditionally, databases behave like a spreadsheet (without 'undo'). 
When you update a cell, or delete a row, the database updates that data 'in place' - the old value is lost.
These are called **'atemporal'** databases - databases which have no built-in concept of row versioning.

Developers with data history or audit requirements usually work around this with well-established but onerous patterns - usually involving either manually updating 'version' or validity columns or copying old rows to a separate history table (some may use automatic triggers to achieve this same aim).
Some set a 'deleted' flag on the rows (a 'soft delete').

When they query the database, they have to bear these workarounds in mind - filtering out rows that are no longer valid. Often, they forget!

### System time

Let's introduce the first temporal dimension: 'system time'.

Some databases are starting to support the concept of system-time (**'unitemporal'** databases) - they track changes to a table, and allow you to view tables as of a time in the past.
Users (rightly) have no control over system-time - the database maintains this timeline.

Rather than a single value, every entity in a unitemporal database has a timeline:

- Let's say we insert a record at time T1 - version 1.
- Later, at time T3, we update that entity to version 2.
- We can then ask the database both "what's the current state of my entity?", and "what was the state of my entity at time T2?"
- If we later delete the entity at time T6, it will no longer be returned as the current state, but we can still time-travel to retrieve the previous versions.

The timeline of this entity is therefore:

- absent for time < T1
- version 1 for T1 ≤ time < T3
- version 2 for T3 ≤ time < T6
- absent for time ≥ T6

System time is often represented in a unitemporal database with 'system from' and 'system to' columns, which represent the time range during which a particular version of a row was current.
If a row is considered current 'until further notice', the 'system to' column is set to null.

So, the timeline of our above entity would be represented in a unitemporal database table as follows:

- Insert at T1 - adds a new row for version 1 with _system_from of T1 and _system_to of null:

  | _id | _system_from | _system_to | version |
  |-|-|-|-|
  | 123 | T1 | null | 1 |
  
- Update at T3 - here, it updates the `_system_to` of the previous version to T3, and adds a new row for version 2:

  | _id | _system_from | _system_to | version |
  |-|-|-|-|
  | 123 | T1 | T3 | 1 |
  | 123 | T3 | null | 2 |
  
- Delete at T6 - it updates the `_system_to` of the previous version to T6:
  | _id | _system_from | _system_to | version |
  |-|-|-|-|
  | 123 | T1 | T3 | 1 |
  | 123 | T3 | T6 | 2 |


You might also hear system-time referred to 'transaction time' or 'processing time'.

### Valid time

System time alone doesn't solve all temporal problems - it's often necessary to additionally track the time period where these facts are considered valid in the real world.

- If you don't become aware of an update until later, you'll want to be able to backdate it to the time it was actually valid.
- You might become aware of an error in the data, and want to correct it retrospectively.

You may even be told about a change in the future: 

- Mike e-mails you saying that he's moving house next week.
- Marketing want to schedule a blog post to go out first thing on Monday.
- The price of one your products is going up next month.

In short, any time you hear the phrase 'as of' or 'with effect from' in a requirement, the answer is probably 'valid time'.

Valid time is the second temporal dimension, making the database **'bitemporal'**.

In practice, valid time is often represented with 'valid from' and 'valid to' columns (in addition to 'system from' and 'system to'), which represent the time range during which a particular version of a row is considered valid in the real world.

You might also hear valid-time referred to as 'business time', 'domain time', 'application time', 'event time', or 'effective time'.
Thank goodness for consistency!

### Bitemporality in XTDB

Bitemporality is ubiquitous in XTDB - every table is bitemporal.

That said, it's opt-in - by default, using normal SQL queries, XTDB looks like it's an atemporal database.

For use cases that don't yet require the full power of bitemporality - here's how normal inserts, updates, queries and deletes work in XTDB:

```sql
INSERT INTO users (_id, user_name, ...) VALUES (?, ?, ...);
-- providing `_id` and `user_name` as separate parameters
-- to avoid SQL injection attacks.

UPDATE users SET user_name = ? WHERE _id = ?;

SELECT * FROM users WHERE user_name = ?;

DELETE FROM users WHERE _id = ?
```

So far, so good.

Nothing remotely bitemporal-looking here, just what you'd write in a traditional database - and 90% of the time, this is what XTDB applications look like.

But, when you need to ask time-oriented questions, here's where XTDB's safety-net kicks in.

These usually take one of the following forms:

1. What's the current state of the world?
2. What's the history of my database, as we now know it (i.e. taking subsequent corrections into account)?
3. What's the history of my database, as we thought it was at the time?

(In our experience, these three categories of questions are in descending order of request frequency, and XTDB optimises accordingly - beneath the surface, we have specific indices to quickly serve current-time queries to get them as close as possible to atemporal performance, and separate indices for historical data.)

To answer these questions, SQL:2011 introduced [an array of new bitemporal primitives](https://dbs.uni-leipzig.de/file/Temporal%20features%20in%20SQL2011.pdf):

1. For category 1: we've chosen to make this the default behaviour in XTDB - query as you normally would.
2. For category 2: when selecting from a table, we can specify a valid time period:

   ```sql
   SELECT * FROM users FOR VALID_TIME AS OF DATE '2023-08-01';
   SELECT * FROM users FOR VALID_TIME BETWEEN DATE '2023-08-01' AND DATE '2023-09-01';
   SELECT * FROM users FOR ALL VALID_TIME;
   ```

3. For category 3: same, but `SYSTEM_TIME`:

   ```sql
   SELECT * FROM users FOR SYSTEM_TIME AS OF DATE '2023-08-01';
   SELECT * FROM users FOR SYSTEM_TIME BETWEEN DATE '2023-08-01' AND DATE '2023-09-01';
   SELECT * FROM users FOR ALL SYSTEM_TIME;
   ```

Inserts, updates and deletes are similar:

* Inserts behave more like an upsert in XTDB.
  If you `INSERT` a row that already exists, no problem - we'll effectively update any existing rows (so that they remains accessible in historical queries), and your new row becomes the current row.

* For updates/deletes in the past/future, use the SQL:2011 `FOR PORTION OF VALID_TIME` syntax

  ```sql
  UPDATE users
  FOR PORTION OF VALID_TIME FROM DATE '2023-08-01' TO DATE '2023-09-01'
  SET user_name = ?
  WHERE _id = ?
  ```

So, for most of the time, for most of your requirements, you can use XTDB like a normal database - but while also being safe in the knowledge that, as your requirements grow, you can incrementally pull in the power of bitemporality when you really need it.

XTDB makes this simple everyday behaviour _easy_ and _fast_, and a wide range of harder bitemporal queries _possible_.

For a detailed specification of the available bitemporal syntax in XTDB, see the SQL [transaction](/reference/main/sql/txs) and [query](/reference/main/sql/queries) reference documentation.

<!-- TODO Basis -->
