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
- You will also need to have the `docs/shared` git submodule checked out. From the root of the project:
  - `git submodule update --init`
- With the submodule initialised, you'll need to pull the objects from `git lfs`. Inside the `docs/shared` directory:
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
