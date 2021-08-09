## Data Model

### Background

Crux was originally based on the idea of storing dynamic, nested documents, with ElasticSearch being the most immediate inspiration. Mongo, Couch and similar stores were other inspirations, both for their ease of initial use, and ability to choose the amount of structure one wants to impose on the data, which varies depending on the problem and the context. On top of this, a Sparql/Datomic-style triple model was provided for indexing and querying.

Despite the initial idea, and ease of storing Nippy-fied nested documents in Crux, nesting has always been a second class citizen. This wasn't by design but accident, as the analysis of matching these two worlds - essentially JSON/EDN documents and EDN Datalog - wasn't done upfront but ad-hoc during the development of Crux. This has led to many oddities in classic.

The type system itself was soon shown to be an issue, and our reliance on Nippy and content hashes led to it being easier to store random Java object graphs into Crux than was ever intended, while also never realising the original intentions of the document-based data model. EDN was deceptively simple to assume as the data model, but is not easy to round trip. We've had several debates about how to solve this in the context of classic, and also considered moving fully to a JSON-first model instead.

### C2 state of affairs

In C2, currently Arrow provides the foundation for its type system, but we currently only support a subset of hard-coded scalar types. Unlike classic, we don't have cardinality-many support, nor support for nesting in general. In practice this means that we can store dynamic flat records, not documents.

This only really applies to the ingest side of C2. The query engine is somewhat accidentally tied to this subset of types, but the original intention has been for it to operate on general Arrow data, and not be strictly tied to the C2 DBMS itself. This is good both because the query engine could be used on its own, but more specifically, as it could be used to query across C2 and external data sources easily.

So, the current state, a subset of types, is there because its the simplest thing we could consider building, and there's been a lot of other work to do. It was never assumed to be the end state.

The goal of being able to decouple the query engine is a more strategic but speculative one, and not strictly necessary for C2 to work, but my assumption is that this will be hard to retrofit if we build in assumptions about the Arrow subset we do support. A lot of the value of Arrow comes from the ability to exchange columnar data in a known format. Without this, we could use some other internal storage format more amenable to nested data. The earlier FlexBuffers spike is an example of this, Apache Ion (used by PartiQL) could be another option.

### C2 way forward

As we have limited time and resources, and also limited interest in debating this forever, but also prefer to defer and allow for incremental work, we need to take these factors into account when weighing our options. To make a decision we need to consider among the following:

1. How much do we really care about nesting?
2. Should nesting be native to the data model and queries, or accessed via special functions (like in SQL:2016 JSON, or classic's maps).
3. Do we agree that querying arbitrary Arrow is a strategic important goal?

Here's a subset of options I made up. They are not really mutually exclusive. Estimates are rough guesses.

#### Do nothing (free, for now)

##### +
+ Frees us up fully to focus elsewhere.

##### -
- Limited type system, no nesting.
- Hard to integrate external Arrow as we make assumptions about the format.
- Once we have users, this will likely require migrations if we want to change it.

#### Decouple the query engine from the DBMS (1 week)

##### +
+ Adds ability to query any external Arrow.
+ Should be able to add nested types incrementally to our indexing later.
+ Arrow query engine as library.

##### -
- Limited type system on ingest, no nesting.
- Requires building some awareness of full Arrow in the expression engine.

#### Add list support (1-2 weeks)

##### +
+ Adds support for lists of the basic scalars, non-nested. SQL Array-like.

##### -
- Limited type system, no proper nesting.
- Hard to integrate external Arrow if we make assumptions about the format.
- Once we have users, this will likely require migrations if we want to change it.

#### Full Arrow internal support (2-4 weeks)

##### +
+ Arguably best state to be in, internal and external data both first class citizens.
+ Full Arrow type system with nesting of both lists and maps.
+ No migrations later.

##### -
- Will take longer to build, larger initial investment.
- Adds complexity where we currently can make assumptions.
- Arrow isn't super suited to lots of dynamic nested data where shapes may differ.
