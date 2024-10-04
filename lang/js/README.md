# XTDB JavaScript/TypeScript client library

This will be the home of the XTDB client library for JavaScript/TypeScript.

In the main, users should use standard PostgreSQL tooling to connect to XTDB - this repo will contain helper functions for advanced usage.

## Publishing

1. Get yourself an account on https://www.npmjs.com/
2. Ask @jarohen for an invite to the `@xtdb` org.
3. `yarn publish --no-git-tag-version`.
   We're currently using version `0.0.0-YYYY.M.D.n` (closest approximation to a Maven snapshot).

### Running postgres tests
```bash
PG_PORT = 5432; # or change port accordingly
yarn run test
```
