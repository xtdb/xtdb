using System.Data;
using Npgsql;
using NpgsqlTypes;
using Xunit;

namespace XtdbTest;

public class XtdbTest : IAsyncLifetime
{
    private readonly NpgsqlDataSource _dataSource;

    public XtdbTest()
    {
        var csb = new NpgsqlConnectionStringBuilder
        {
            Host = Environment.GetEnvironmentVariable("PG_HOST") ?? "localhost",
            Port = int.Parse(Environment.GetEnvironmentVariable("PG_PORT") ?? "5439"),
            Database = Guid.NewGuid().ToString(),
            Username = "xtdb",
            MaxPoolSize = 1,
        };

        var builder = new NpgsqlDataSourceBuilder(csb.ConnectionString);
        builder.ConfigureTypeLoading(sb =>
        {
            sb.EnableTypeLoading(false);
            sb.EnableTableCompositesLoading(false);
        });
        _dataSource = builder.Build();
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync() => await _dataSource.DisposeAsync();

    private NpgsqlCommand Command(NpgsqlConnection conn, string sql, params object[] args)
    {
        var cmd = new NpgsqlCommand(sql, conn);
        foreach (var arg in args)
            cmd.Parameters.Add(arg as NpgsqlParameter ?? new NpgsqlParameter { Value = arg });
        return cmd;
    }

    private async Task Execute(string sql, params object[] args)
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = Command(conn, sql, args);
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task<T> QueryOne<T>(string sql, params object[] args)
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = Command(conn, sql, args);
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        var value = reader.GetFieldValue<T>(0);
        Assert.False(await reader.ReadAsync());
        return value;
    }

    private async Task AssertRoundTrips<T>(T value)
    {
        await Execute("INSERT INTO docs (_id, v) VALUES (1, $1)", new NpgsqlParameter<T> { TypedValue = value });
        Assert.Equal(value, await QueryOne<T>("SELECT v FROM docs WHERE _id = 1"));
    }

    [Fact]
    public async Task Connects() =>
        Assert.Equal(1L, await QueryOne<long>("SELECT 1 AS value"));

    [Fact]
    public async Task InsertsAndReadsBackARow()
    {
        await Execute("INSERT INTO foo (_id, msg) VALUES ($1, $2)", 1L, "Hello world!");

        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = Command(conn, "SELECT _id, msg FROM foo");
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(1L, reader.GetInt64(0));
        Assert.Equal("Hello world!", reader.GetString(1));
    }

    [Fact]
    public Task RoundTripsBool() => AssertRoundTrips(true);

    [Fact]
    public Task RoundTripsInt8() => AssertRoundTrips(666L);

    [Fact]
    public Task RoundTripsFloat8() => AssertRoundTrips(666.666);

    [Fact]
    public Task RoundTripsText() => AssertRoundTrips("test-foo");

    [Fact]
    public Task RoundTripsUuid() => AssertRoundTrips(Guid.NewGuid());

    [Fact]
    public Task RoundTripsDate() => AssertRoundTrips(new DateOnly(2020, 1, 1));

    [Fact]
    public Task RoundTripsTimestamptz() =>
        AssertRoundTrips(new DateTime(2020, 1, 1, 12, 34, 56, DateTimeKind.Utc));

    [Fact]
    public Task RoundTripsBytea() => AssertRoundTrips(new byte[] { 0, 1, 255, 92, 120 });

    [Fact]
    public Task RoundTripsInt8Array() => AssertRoundTrips(new[] { 1L, 2L, 3L });

    [Fact]
    public Task RoundTripsTextArray() => AssertRoundTrips(new[] { "a", "b", "c" });

    [Fact]
    public async Task DecodesBinaryJsonbParams()
    {
        await Execute("INSERT INTO docs (_id, doc) VALUES (1, $1)",
            new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Jsonb, Value = """{"a": 1}""" });

        Assert.Equal(1L, await QueryOne<long>("SELECT (doc).a FROM docs"));
    }

    [Fact]
    public async Task DecodesBinaryIntervalParams() =>
        Assert.Equal(
            new DateTime(2020, 1, 2, 1, 1, 1, DateTimeKind.Utc),
            await QueryOne<DateTime>("SELECT TIMESTAMP '2020-01-01 00:00:00+00:00' + $1",
                new TimeSpan(1, 1, 1, 1)));

    [Fact]
    public async Task MatchesCaseInsensitivelyWithIlike()
    {
        Assert.True(await QueryOne<bool>("SELECT 'Hello' ILIKE 'hel%'"));
        Assert.False(await QueryOne<bool>("SELECT 'Hello' NOT ILIKE 'hel%'"));
    }

    [Fact]
    public async Task DecimalParamsDescribeNumericResults()
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = Command(conn, "SELECT $1 AS d, $2 + 1 AS e, 1 AS i", 1.5m, 2.5m);
        await using var reader = await cmd.ExecuteReaderAsync();

        Assert.Equal(typeof(decimal), reader.GetFieldType(0));
        Assert.Equal(typeof(decimal), reader.GetFieldType(1));
        Assert.Equal(typeof(long), reader.GetFieldType(2));

        Assert.True(await reader.ReadAsync());
        Assert.Equal(1.5m, reader.GetDecimal(0));
        Assert.Equal(3.5m, reader.GetDecimal(1));
        Assert.Equal(1L, reader.GetInt64(2));
    }

    [Fact]
    public async Task ArrayParamsKeepResultColumnTypes()
    {
        await Execute("INSERT INTO docs RECORDS {_id: 1, name: 'a'}");

        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = Command(conn, "SELECT _id, name FROM docs WHERE _id = ANY($1)", new[] { 1L });
        await using var reader = await cmd.ExecuteReaderAsync();

        Assert.Equal(typeof(long), reader.GetFieldType(0));
        Assert.Equal(typeof(string), reader.GetFieldType(1));

        Assert.True(await reader.ReadAsync());
        Assert.Equal(1L, reader.GetInt64(0));
        Assert.Equal("a", reader.GetString(1));
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task CastsToPostgresScalarTypeNames()
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = Command(conn, "SELECT 1::int4, 1::int8, 1.5::float8, TRUE::bool");
        await using var reader = await cmd.ExecuteReaderAsync();

        Assert.True(await reader.ReadAsync());
        Assert.Equal(1, reader.GetInt32(0));
        Assert.Equal(1L, reader.GetInt64(1));
        Assert.Equal(1.5, reader.GetDouble(2));
        Assert.True(reader.GetBoolean(3));
    }

    [Fact]
    public async Task CastsToListTypes()
    {
        Assert.Equal(new[] { 1L, 2L }, await QueryOne<long[]>("SELECT $1::int8[]", new[] { 1L, 2L }));
        Assert.Equal(new[] { 1L, 2L }, await QueryOne<long[]>("SELECT '{1,2}'::BIGINT ARRAY"));
        Assert.Equal(new[] { "x" }, await QueryOne<string[]>("SELECT $1::VARCHAR[]", (object)new[] { "x" }));
    }

    [Fact]
    public async Task PooledConnectionsAreResetOnReuse()
    {
        int pid;
        string defaultTz;

        await using (var conn = await _dataSource.OpenConnectionAsync())
        {
            pid = conn.ProcessID;
            await using var show = Command(conn, "SHOW TIME ZONE");
            defaultTz = (string)(await show.ExecuteScalarAsync())!;

            await using var set = Command(conn, "SET TIME ZONE 'Asia/Tokyo'");
            await set.ExecuteNonQueryAsync();
        }

        await using (var conn = await _dataSource.OpenConnectionAsync())
        {
            Assert.Equal(pid, conn.ProcessID);
            await using var show = Command(conn, "SHOW TIME ZONE");
            Assert.Equal(defaultTz, await show.ExecuteScalarAsync());
        }
    }

    [Fact]
    public async Task ReportsColumnsInPostgresTerms()
    {
        await Execute("INSERT INTO docs RECORDS {_id: 1, s: 'a', ts: TIMESTAMP '2020-01-01 00:00:00+00:00', ints: [1, 2]}");

        Assert.Equal("bigint", await QueryOne<string>(
            "SELECT data_type FROM information_schema.columns WHERE table_name = 'docs' AND column_name = '_id'"));

        await using var conn = await _dataSource.OpenConnectionAsync();
        var columns = await conn.GetSchemaAsync("Columns", [null, null, "docs"]);

        var dataTypes = columns.Rows.Cast<DataRow>()
            .ToDictionary(r => (string)r["column_name"], r => (string)r["data_type"]);

        Assert.Equal("bigint", dataTypes["_id"]);
        Assert.Equal("text", dataTypes["s"]);
        Assert.Equal("timestamp with time zone", dataTypes["ts"]);
        Assert.Equal("bigint[]", dataTypes["ints"]);
    }
}
