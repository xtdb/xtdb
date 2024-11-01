# Elixr

This is essentially a test suite for pgwire protocol with binary encoding.
Elixir happens to be the language supporting it natively.
We use the elixir-ecto/postgrex client for interacting with XTDB from the elixr_test.exs

## Running

Install dependencies by running `mix deps.get` and then `mix test`
If you need to override the host or port you can set environment variables:
- PG_HOST
- PG_PORT
eg. like so: `PG_PORT=5439 mix test`

The test suite is meant to run as part of CI - hence the Dockerfile and corresponding entry in docker-compose.
