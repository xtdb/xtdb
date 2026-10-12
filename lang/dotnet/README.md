# .NET

An xUnit suite for XTDB through [Npgsql](https://www.npgsql.org/), the PostgreSQL driver underneath most of .NET's database tooling (Dapper, EF Core).
Each test connects to a fresh database.

`XtdbTest.cs` exercises Npgsql directly; `EfCoreTest.cs` runs LINQ queries through an EF Core `DbContext` over the same connection settings.

## Running

With the .NET 10 SDK installed, `dotnet test`.
`PG_HOST` and `PG_PORT` override the host and port, e.g. `PG_PORT=5439 dotnet test`.

The suite is meant to run as part of CI - hence the Dockerfile and corresponding entry in docker-compose.
After changing a package version, `dotnet restore --force-evaluate` rewrites `packages.lock.json`, which the Docker build restores against.
