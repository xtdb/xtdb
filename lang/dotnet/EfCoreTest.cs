using Microsoft.EntityFrameworkCore;
using Npgsql;
using Xunit;

namespace XtdbTest;

public class Person
{
    public long Id { get; set; }
    public string Name { get; set; } = "";
    public long Age { get; set; }
    public long? CityId { get; set; }
}

public class City
{
    public long Id { get; set; }
    public string Name { get; set; } = "";
}

public class PeopleContext(NpgsqlDataSource dataSource) : DbContext
{
    public DbSet<Person> People => Set<Person>();
    public DbSet<City> Cities => Set<City>();

    protected override void OnConfiguring(DbContextOptionsBuilder options) => options.UseNpgsql(dataSource);

    protected override void OnModelCreating(ModelBuilder model)
    {
        model.Entity<Person>(e =>
        {
            e.ToTable("people");
            e.Property(p => p.Id).HasColumnName("_id").ValueGeneratedNever();
            e.Property(p => p.Name).HasColumnName("name");
            e.Property(p => p.Age).HasColumnName("age");
            e.Property(p => p.CityId).HasColumnName("city_id");
        });

        model.Entity<City>(e =>
        {
            e.ToTable("cities");
            e.Property(c => c.Id).HasColumnName("_id").ValueGeneratedNever();
            e.Property(c => c.Name).HasColumnName("name");
        });
    }
}

public class EfCoreTest : IAsyncLifetime
{
    private readonly NpgsqlDataSource _dataSource;

    public EfCoreTest()
    {
        var csb = new NpgsqlConnectionStringBuilder
        {
            Host = Environment.GetEnvironmentVariable("PG_HOST") ?? "localhost",
            Port = int.Parse(Environment.GetEnvironmentVariable("PG_PORT") ?? "5439"),
            Database = Guid.NewGuid().ToString(),
            Username = "xtdb",
            MaxPoolSize = 1,
        };

        _dataSource = NpgsqlDataSource.Create(csb.ConnectionString);
    }

    public async Task InitializeAsync()
    {
        string[] seeds =
        [
            "INSERT INTO cities RECORDS {_id: 1, name: 'London'}, {_id: 2, name: 'Paris'}",
            "INSERT INTO people RECORDS {_id: 1, name: 'Alice', age: 30, city_id: 1}, {_id: 2, name: 'Bob', age: 25, city_id: 2}, {_id: 3, name: 'Carol', age: 35}",
        ];

        await using var conn = await _dataSource.OpenConnectionAsync();
        foreach (var sql in seeds)
        {
            await using var cmd = new NpgsqlCommand(sql, conn);
            await cmd.ExecuteNonQueryAsync();
        }
    }

    public async Task DisposeAsync() => await _dataSource.DisposeAsync();

    private PeopleContext Context() => new(_dataSource);

    [Fact]
    public async Task FiltersOnACapturedLocal()
    {
        var minAge = 30L;
        await using var ctx = Context();

        Assert.Equal(new[] { "Alice", "Carol" },
            await ctx.People.Where(p => p.Age >= minAge).OrderBy(p => p.Id).Select(p => p.Name).ToListAsync());
    }

    [Fact]
    public async Task OrdersAndPages()
    {
        await using var ctx = Context();

        Assert.Equal(new[] { 1L }, await ctx.People.OrderBy(p => p.Age).Skip(1).Take(1).Select(p => p.Id).ToListAsync());
    }

    [Fact]
    public async Task FindsOneByKey()
    {
        await using var ctx = Context();

        Assert.Equal("Bob", (await ctx.People.FirstOrDefaultAsync(p => p.Id == 2))?.Name);
    }

    [Fact]
    public async Task Aggregates()
    {
        await using var ctx = Context();

        Assert.Equal(3, await ctx.People.CountAsync());
        Assert.True(await ctx.People.AnyAsync(p => p.Age > 30));
        Assert.Equal(35L, await ctx.People.MaxAsync(p => p.Age));
    }

    [Fact]
    public async Task MatchesStringsWithLikeAndIlike()
    {
        await using var ctx = Context();

        Assert.Equal(new[] { "Alice" }, await ctx.People.Where(p => p.Name.Contains("lic")).Select(p => p.Name).ToListAsync());
        Assert.Equal(new[] { "Alice" }, await ctx.People.Where(p => p.Name.StartsWith("Al")).Select(p => p.Name).ToListAsync());
        Assert.Equal(new[] { "Alice" }, await ctx.People.Where(p => EF.Functions.ILike(p.Name, "al%")).Select(p => p.Name).ToListAsync());
    }

    [Fact]
    public async Task JoinsTwoTables()
    {
        await using var ctx = Context();

        var rows = await ctx.People
            .Join(ctx.Cities, p => p.CityId, c => c.Id, (p, c) => new { Person = p.Name, City = c.Name })
            .OrderBy(r => r.Person)
            .ToListAsync();

        Assert.Equal(new[] { new { Person = "Alice", City = "London" }, new { Person = "Bob", City = "Paris" } }, rows);
    }

    [Fact]
    public async Task GroupsAndCounts()
    {
        await using var ctx = Context();

        var counts = await ctx.People
            .GroupBy(p => p.CityId)
            .Select(g => new { CityId = g.Key, Count = g.Count() })
            .ToDictionaryAsync(g => g.CityId ?? 0, g => g.Count);

        Assert.Equal(new Dictionary<long, int> { [0] = 1, [1] = 1, [2] = 1 }, counts);
    }

    [Fact(Skip = "pgwire reports auto-committed DML as `INSERT 0 0`, so SaveChanges's rows-affected check throws DbUpdateConcurrencyException although the write lands (#3506)")]
    public async Task SaveChangesWritesAreReadBack()
    {
        await using (var ctx = Context())
        {
            ctx.People.Add(new Person { Id = 10, Name = "Dan", Age = 40 });
            await ctx.SaveChangesAsync();

            (await ctx.People.SingleAsync(p => p.Id == 1)).Age = 31;
            await ctx.SaveChangesAsync();

            ctx.People.Remove(await ctx.People.SingleAsync(p => p.Id == 2));
            await ctx.SaveChangesAsync();
        }

        await using var fresh = Context();
        Assert.Equal("Dan", (await fresh.People.SingleAsync(p => p.Id == 10)).Name);
        Assert.Equal(31L, (await fresh.People.SingleAsync(p => p.Id == 1)).Age);
        Assert.False(await fresh.People.AnyAsync(p => p.Id == 2));
    }
}
