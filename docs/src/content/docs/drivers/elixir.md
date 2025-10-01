---
title: Using XTDB from Elixir
---

In Elixir, you can talk to a running XTDB node using the [Postgrex](https://github.com/elixir-ecto/postgrex) driver and XTDB's Postgres wire-compatibility.

## Install

To install the Postgrex driver, add the following dependency to your Elixir project's `mix.exs`:

``` elixir
defp deps do
  [
    {:postgrex, "~> 0.16.5"}
  ]
end
```

## Connect

Once you've [started your XTDB node](/intro/installation-via-docker), you can use the following code to connect to it:

``` elixir
defmodule XTDBExample do
  def connect_and_query do
    {:ok, pid} = Postgrex.start_link(
      hostname: "localhost",
      port: 5432,
      database: "xtdb"
    )

    insert_query = """
    INSERT INTO users RECORDS {_id: 'jms', name: 'James'}, {_id: 'joe', name: 'Joe'}
    """
    select_query = "SELECT * FROM users"

    Postgrex.query(pid, insert_query, [])

    {:ok, %Postgrex.Result{rows: rows}} = Postgrex.query(pid, select_query, [])

    IO.puts("Users:")
    Enum.each(rows, fn [id, name] -> IO.puts("  * #{id}: #{name}") end)
  end
end

"""
Example output:

iex(1)> XTDBExample.connect_and_query()
Users:
  * joe: Joe
  * jms: James
:ok

"""
```
