## Bitemp

### Why?

In Datomic it's very tempting to try to use the single time line as
business time, but as one quickly realises, this is false economy.

For simple standalone or CRUD systems where Datomic ultimately owns
the data, the business time and transaction time can more or less be
treated as the same, and Datomic's view of time works.

In any situation where Datomic isn't the ultimate owner of the data,
and where corrections to data can flow in from various sources, and at
various times, using Datomic's single time line as business time does
not work. At this point one gets forced to model business time
separately in the schema itself. This is doable, but far from trivial,
as you need to ensure you can see the latest version of each entity
across both time lines at once. Various solutions involving filtering
the database have been suggested to this problem.

So Datomic's single time line works well for the transaction
time. This time line is usually not interesting from a business POV,
but can be very useful for auditing, event sourcing and other more
technical use cases. It's also fundamental as you cannot replace this
time line with a single business time line, as you would then have a
normal, mutable store where reads cannot be guaranteed to stay
consistent.

The ability to write in the past or correct it is key to many real
world business systems. These business requirements cannot be reasoned
away with that "you cannot go back in time". You need to record both
times, the business time of the change, and the transaction time when
this change took place.

Still, we assume that in most cases, this isn't the "normal" way of
using the system, that is, most data would be written only once.

**In a ecosystem of many systems, where one cannot control the
ultimate time line, or other systems abilities to write into the past,
one needs bitemporality to ensure evolving but consistent views of the
data.**

### Business Time

In Crux, most queries would consider themselves with business time,
and business time only. Only if you want to ensure repeatable reads or
do auditing queries, would you concern yourself with both time
lines. So normally you ask, what is the value of this entity at time
T-business, regardless if this history has been rewritten several
times from a transaction time POV before time of query. The
transaction time is hence simply now for most normal queries.

Similarly, when writing data, in case there isn't any specific
business time available, the business time and transaction time take
the same value. This is like the simple case above when using
Datomic's single time line and having control over the data.

### Transaction Time

As mentioned above, transaction time is just the time the change was
recorded in the system. It will always exist, but usually not be
queried, unless for repeatable read, audit or change tracking
purposes.

### Point vs Range

One issue with writing into the past, is that it can be hard to reason
about what you actually change. If one overwrites an earlier
change with exactly the same business time, one would effectively
correct this value. But if one writes a value into the past at a
business time without a previous value, one would effectively insert a
new version of the entity into the past.

When overwriting one value in the past, the system needs to be
predictable about what happens to the next value. By default this
value wouldn't change, but there might need to be possible to change a
range of values across business time in a consistent manner. The
changes would need to be done in a transaction, see
[transaction.md](transaction.md).

### Event Sourcing

Writing into the past assumes that any downstream system that derives
state from the transaction log can deal with this. This is not
necessarily a safe assumption. Often writes in the past, or
corrections, are events in their own regard, which raises questions
about what we actually write into our log, raw assertions or versions
of documents, or actual events from which these are derived?
