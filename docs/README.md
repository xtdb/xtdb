# XTDB Docs

This is a static site based on the [Astro framework](https://astro.build), using the [Starlight](https://starlight.astro.build/) documentation template.
The project structure follows a standard [Astro structure](https://docs.astro.build/en/core-concepts/project-structure/).

## Pre-requisites

To work on this project you should have `git lfs` installed and initialised.

- To install, use your OS package manager:
  - For Arch: `sudo pacman -Sy git-lfs`
  - For Mac OS: `brew install git-lfs`
- You will need to initialise `git lfs`:
  - `git lfs install`
- You will also need to have the `docs/shared` git submodule checked out.
  From the root of the project:
  - `git submodule update --init`
- With the submodule initialised, you'll need to pull the objects from `git lfs`.
  Inside the `docs/shared` directory:
  - `git lfs pull`

From this point you should be able to run the commands in the next section.

## Commands

All commands are run from the `docs` folder of the project, from a terminal:

| Command | Action |
| --- | --- |
| `yarn install` | Installs dependencies |
| `yarn run dev` | Starts local dev server at `localhost:4321` |
| `yarn run build` | Build your production site to `./dist/` |
| `yarn run preview` | Preview your build locally, before deploying |
| `yarn run astro ...` | Run CLI commands like `astro add`, `astro check` |
| `yarn run astro --help` | Get help using the Astro CLI |
| `yarn run build-api-docs` | Builds API docs for Clojure to `dist/clojure-docs`. |

## Reference documentation versioning

To create a new reference documentation version, `./bin/snapshot-reference-docs.sh <new-version>`.

## Deploy

We use [AWS Amplify](https://aws.amazon.com/amplify/hosting/) to host the static site.

To deploy to the live site, push to the `docs-live` branch on GitHub.

## Writing docs

End-user documentation lives under `docs/src/content/docs/`, organised around the [Diataxis](https://diataxis.fr/) framework.
The conventions below keep pages coherent across contributors (human and AI), and are also what an AI agent should follow when editing or adding pages.

### Diataxis

The four Diataxis quadrants — tutorials, how-to, reference, explanation — apply recursively.
At the top level, each directory leans toward one of them:

| Directory | Leans toward | Shape |
| --- | --- | --- |
| `intro/`, `quickstart/`, `tutorials/` | Tutorials | Guided walkthrough with a working example |
| `about/`, `concepts/` | Explanation | Builds the reader's mental model — no task, no commands |
| `ops/`, `guides/` | How-to | Numbered steps that solve a specific operator problem |
| `reference/main/` | Reference | Terse, exhaustive, look-up shape |
| `drivers/`, `xtql/` | Mixed | Usually some combination; match per-page |

Within a sub-component, the quadrants repeat at a smaller scale.
`ops/config/log/kafka.md` is how-to overall, but internally mixes all four: a brief intro (explanation of what Kafka is for in XTDB), numbered setup steps (how-to), a commented YAML block (reference), and a "Leader election and fencing" section (explanation of mechanism).
Each sub-section does one quadrant's job; they don't blur together.

The test isn't "where does this page live?", it's "what is *this paragraph/section* for?".
Adding a new topic?
Decide which quadrants it needs and structure the page so each section does one thing.
CDC source setup is a how-to; what an external-source database *is* is an explainer; how CDC tokens get persisted is a reference detail.

### Voice

- Conversational "we" / "you".
  Assume a technical, database-literate reader — don't re-explain basics like ACID or DML.
- One sentence per line in prose sections.
- Use markdown definition lists (`Term\n\n: definition…`) for defining components, YAML fields, and changelog bodies.
- Inline version markers like `(v2.1+)` / `(v2.2+)` on section headings and YAML comments for features added in specific releases.
- Concrete examples, not placeholders (`'my-kafka'`, not `'cluster-name'`).
- Site-relative cross-links (`/ops/config/log`, `/reference/main/sql/queries#basis`).
- No emoji.
  No marketing fluff.
  No `(unreleased)` markers — users check the latest published version.

### Changelog conventions

Affected pages carry a collapsible `<details>` block right under the frontmatter:

````md
<details>
<summary>Changelog (last updated vX.Y)</summary>

vX.Y: short headline

: One-sentence lead on the current state, linking to the main-body section that covers it in full.

  Previously X; now Y.
  Explain why it changed.

  Upgrade steps, if applicable — or explicitly state there are none.

vX.Y-1: earlier headline

: Earlier entry.

</details>
````

The main page body describes *current* behaviour only — write as if the feature has always been there.
The changelog carries the *transition* story — what it was, why it changed, how to upgrade.

Avoid narrative phrases in the main body like "now", "under v2.2+", "previously", "with this change", "the default handles this transparently" — these belong in the changelog.
Inline `(vX.Y+)` markers on section headings are the right place to flag when a feature arrived, without narrating the change.

Example — a Helm chart that gained an extra Kafka topic in v2.2:

Wrong (main body, narrates the transition):

```md
Under single-writer indexing (v2.2+), each database uses two Kafka topics — a source log and a replica log.
The default chart handles this transparently: the replica topic defaults to `${kafkaLogTopic}-replica` and auto-creates alongside the source topic.
Most single-deployment setups don't need to do anything.
```

Right (main body, states the current fact):

```md
Kafka topics used by the chart (v2.2+):

- `xtdbConfig.kafkaLogTopic` — source log topic.
- `xtdbConfig.kafkaReplicaLogTopic` — replica log topic.
```

The "previously X, now Y, how to upgrade" content for that feature lives in the changelog block at the top.

### Changelog entry vs inline version marker

Not every v2.X feature needs a changelog entry.

- **Changelog entry** — for *transitions*, i.e. changes with a previous state to document.
  Single-writer indexing replaced multi-writer indexing — that's a transition.
- **Inline `(vX.Y+)` marker** — for purely *additive* features.
  Read-only secondaries and `XTDB_SKIP_DBS` added new capabilities but didn't change anything that existed before, so they live inline on their section heading (`### Read-only secondary databases (v2.2+)`) without a dedicated changelog entry.

### Properties vs mechanism

Conceptual pages (`about/`, `concepts/`) describe *properties the system provides*.
Plugin-specific pages (`ops/config/log/kafka.md`, `ops/config/storage.md`) describe *how those properties are enforced by this particular plugin*.

e.g. `about/dbs-in-xtdb.md` says "exactly one leader per database at any given time, elected automatically".
The Kafka page owns "via consumer-group rebalancing, fenced via transactional producers, prefixed with `transactionalIdPrefix`".
Don't leak Kafka jargon into the conceptual page.

### Name operator-visible concepts

If an operator will see a thing in config — a Kafka topic, an environment variable, an object-store prefix — name it.
The source log and replica log are separate Kafka topics; an operator setting up Kafka sees both, so the docs name them rather than saying "the leader's output" and leaving the reader to reverse-engineer what it means at the Kafka level.

### Scope carefully

Watch for framings that over-generalise.
XTDB's single-writer model elects a leader *per database* — a single node may be leader for database A and follower for database B at the same time.
Phrases like "a single leader in the cluster" imply cluster-wide leadership and are wrong.
Prefer "the leader for that database", "this database's indexing leader".

### Diagrams

Prefer inline D2 over static SVGs — D2 is source-controlled, searchable, and easy to iterate on.

Style for light + dark mode: use structural emphasis (`style.bold: true`, `style.stroke-width`, nesting) rather than colour fills.
A fill like `#fde68a` that looks good in one theme can look wrong in the other.

### Before documenting a feature

Read:

- the feature's landing commit body (`git log --all --grep='<feature>'`);
- its pull request — description *and* review discussion (`gh pr view N`, `gh pr view N --comments`);
- if chalk was active, its tracking issue and chalk comments (`gh issue view N --comments`).

All three carry the *why* that isn't visible in the diff — root causes, operational guarantees, invariants, rejected alternatives — and noticeably improve the resulting docs.

e.g. the `XTDB_SKIP_DBS` troubleshooting note originally said "the node will fail to start if a secondary's log is unavailable".
The commit body spelled out *why* (`DatabaseCatalog.open()` attaches every secondary from the block catalog), and the chalk comment noted that `healthz` is multi-db-aware and naturally ignores dormant databases.
Both details were operationally useful and became part of the final docs.
