## Data Retention

### GDPR and Datensparsamkeit

Many systems today will be built with
[GDPR](https://en.wikipedia.org/wiki/General_Data_Protection_Regulation)
and "the right to be forgotten" (later, "right to erasure") in mind
with regards to personal data. Further existing systems will need to
be retrofitted with these principles in mind.

A related concept is the German
[Datensparsamkeit](https://martinfowler.com/bliki/Datensparsamkeit.html),
which is the principle to only retain the data you actually need.

All this is a counter weight to the previous trend where various
algorithms prefer to keep as much data as possible about everything
and attempt to learn patterns within this data.

**Data you don't store cannot be misused (intentionally or otherwise)
nor stolen.**

+ [Handling GDPR with Apache Kafka: How does a log
  forget?](https://www.confluent.io/blog/handling-gdpr-log-forget/)

### Event Sourcing

The immutable log is a powerful and clean functional model of a
system. By definition, mandatory deletions will force a rethink of
this model.

Forgetting can by definition not be done simply via a bitemporal
correction in a system built around an immutable log. One approach
that has been suggested to deal with this is pseudonymisation, where
you keep encrypted data but forget the keys, which are stored in a
separate mutable store.

+ [The Log: What every software engineer should know about real-time
  data's unifying
  abstraction](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying)
+ [The Data Dichotomy: Rethinking the Way We Treat Data and
  Services](https://www.confluent.io/blog/data-dichotomy-rethinking-the-way-we-treat-data-and-services/)

### Provenance

Data will always come from somewhere, and the retention rules might be
defined upstream. Most likely other downstream systems, if nothing
else, Crux own indexes, will been derived from the log itself. When
something is deleted, it will also need to be purged from these
systems. Sometimes that might not be possible, which might inform the
decision of feeding those systems in the first place.

This meta-data would likely be part of the schema itself, and stored
together with either triplets or documents. Not all data might
necessarily need this fine grained level of provenance.

### Log Compaction

There are other (at times related) reasons why one would want to
delete or compact the log in various ways. Maybe no data older than a
year needs to be stored, or it can simply be archived. Maybe the
frequency of changes further back in history can be condensed, so one
compacts them to say daily changes. All this is complicated by the
schema and data model, and how one decides what is reachable at a
certain point in time. See both [schema](schema.md) and
[bitemp](bitemp.md) for more.

Crux could at the lowest level provide the tools that helps one
compact the log in a consistent manner. It could also potentially also
do this on its own, based on provenance and retention meta-data.

Compacting the log is intimately related to building the indexes, as
it's only in the realised indexes that one can get the correct
bitemporal visibility needed to compact the data as of a certain point
in time.

### Log Replay

If downstream systems, especially Crux own indexes, depend on a
immutable log, replaying this from start might eventually become
impractical or even impossible. Depending on the issue, this can be
mitigated downstream or dealt with indirectly by compacting the log,
as discussed above.

**It's assumed that the replay of the log can be resumed, so for
normal usage replaying the entire log should not be necessary, but can
be dealt with incremental backups at the downstream systems.**
