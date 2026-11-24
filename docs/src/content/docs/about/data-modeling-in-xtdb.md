---
title: Data modeling in XTDB
---

XTDB doesn't ask you to declare a schema before you write to it, but a few modeling choices still shape how your data is laid out and how quickly it reads back.

## Choosing IDs

Every row you write carries an ID - the value of its table's primary key - and XTDB uses it to tie the versions of a record together.
You choose the IDs - XTDB doesn't generate them.

A table's primary key is `_id` unless the table declares another column:

```sql
CREATE TABLE orders (order_no PRIMARY KEY, customer);
```

Declare the key before anything is written to the table: the first write fixes it, and from then on it can't change.
A key is a single column.
See [Primary keys](/reference/main/sql/schema#primary-keys-v221) for the rules.

XTDB doesn't store your rows in ID order.
It derives a 16-byte internal ID from each row's key value, and partitions each table's data by that internal ID.

- A UUID key value is used as the internal ID unchanged.
- Any other key value - a string, an integer, a keyword - is hashed to give its internal ID.

So hashed IDs are spread evenly across a table whatever values you choose, while UUIDs are laid out exactly as you generate them.
That gives you control over where a row is stored, and with it a responsibility to spread your UUIDs evenly.

See [SQL Transactions](/reference/main/sql/txs#id-types) for the types an ID can take.

### Spread UUIDs evenly

XTDB divides a table's data up by the leading bits of the internal ID.
Where most UUIDs share their leading bits, their rows land together however the table is divided, so the files holding them keep growing instead of being split up.

Random UUIDs (version 4) are spread evenly, and are the right default.

Time-ordered UUIDs are not.
A version 7 UUID - like a ULID - begins with a millisecond timestamp, so every ID you generate in the same period shares its leading bits.

XTDB already indexes your data by time - system time and valid time - so a time-ordered ID doesn't buy you anything here.
If an existing system hands you time-ordered UUIDs, either generate a random UUID for the key and keep theirs as an ordinary column, or write theirs as a string, which XTDB hashes.

### Keep related records together

Because UUIDs aren't hashed, you can place records next to each other by giving them UUIDs that share their leading bytes.
Give an order's line items UUIDs that start with the order's own leading bytes, and fetching the line items for that order reads one part of the table rather than several.

The shared prefix needs to be evenly spread itself - take it from a random UUID, such as the parent record's, and fill the rest of each ID randomly.

How many bytes are worth sharing depends on your data: how large the table is, and how much of it is history rather than current.
A group that shares a prefix concentrates its records just as time-ordered IDs do, so keep each group small relative to the table.
