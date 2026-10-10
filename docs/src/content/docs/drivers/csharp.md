---
title: Using XTDB from C#
---

In C#, you can talk to a running XTDB node using the [Npgsql](https://www.npgsql.org/) PostgreSQL driver, taking advantage of XTDB's PostgreSQL wire-compatibility.

## Install

To install Npgsql, add it to your project:

```bash
dotnet add package Npgsql
```

Or via NuGet Package Manager:

```
Install-Package Npgsql
```

## Connect

Once you've [started your XTDB node](/intro/installation-via-docker), you can use the following code to connect to it:

```csharp
using System;
using System.Threading.Tasks;
using Npgsql;

class XtdbExample
{
    static async Task Main()
    {
        var connectionString = "Host=localhost;Port=5432;Database=xtdb;Username=xtdb;Password=xtdb;";

        var dataSourceBuilder = new NpgsqlDataSourceBuilder(connectionString);

        // Required for XTDB support
        dataSourceBuilder.ConfigureTypeLoading(sb => {
            sb.EnableTypeLoading(false);
            sb.EnableTableCompositesLoading(false);
        });

        var dataSource = dataSourceBuilder.Build();
        var connection = await dataSource.OpenConnectionAsync();

        await using (var insertCommand = connection.CreateCommand())
        {
            insertCommand.Parameters.Add(new NpgsqlParameter()).Value = "Alice";
            insertCommand.CommandText = "INSERT INTO users (_id, name) VALUES (1, ?)";
            await insertCommand.ExecuteNonQueryAsync();
        }

        await using (var queryCommand = connection.CreateCommand())
        {
            queryCommand.CommandText = "SELECT _id, name FROM users";
            await using var reader = await queryCommand.ExecuteReaderAsync();

            Console.WriteLine("Users:");
            while (await reader.ReadAsync())
            {
                var id = reader.GetInt32(0);
                var name = reader.GetString(1);
                Console.WriteLine($"  * {id}: {name}");
            }
        }

        Console.WriteLine("\n✓ XTDB connection successful");
    }
}

/* Output:

Users:
  * 1: Alice

✓ XTDB connection successful
*/
```

## Connection pooling (v2.2+)

Npgsql resets a pooled connection with `DISCARD ALL` before handing it to its next user.
XTDB returns the session's time zone, `SET` parameters, await token and default access mode to their startup values, and closes the session's prepared statements and portals.
`DISCARD ALL` is refused inside an open transaction.

`DISCARD PLANS` closes the prepared statements and portals only; `DISCARD TEMP` and `DISCARD SEQUENCES` are accepted and do nothing.

## Examples

For more examples and tests, see the [XTDB driver-examples repository](https://github.com/xtdb/driver-examples), which contains comprehensive test suites demonstrating various features and use cases.
