defmodule ElixrTest do
  use ExUnit.Case

  setup do
    {port, _rest} = Integer.parse(System.get_env("PG_PORT", "5439"))
    host = System.get_env("PG_HOST", "localhost")
    {:ok, pid} = Postgrex.start_link(
      hostname: host, port: port, database: "xtdb", username: "xtdb")
    {:ok, xt: pid}
  end

  test "connection is made to XTDB", state do
    refute state[:xt] == nil
  end

  test "should insert a row and be able to read it too", state do
    pid = state[:xt]
    Postgrex.query!(pid, "INSERT INTO foo0 (_id, bar) VALUES (0, 'baz')", [])
    result = Postgrex.query!(pid, "SELECT * FROM foo0", [])
    assert [[0, "baz"]] == result.rows
  end

  test "should be able to read an array of int8", state do
    pid = state[:xt]
    result = Postgrex.query!(pid, "SELECT ARRAY[1,2,3]", [])
    assert [[[1, 2, 3]]] == result.rows
  end

  test "should be able to read an array of int4", state do
    pid = state[:xt]
    result = Postgrex.query!(pid, "SELECT ARRAY[1, 2, 3]", [])
    assert [[[1, 2, 3]]] == result.rows
  end

  test "inserts/retrieves boolean", state do
    pid = state[:xt]
    Postgrex.query!(pid, "INSERT INTO foo1 (_id, bool) VALUES (1, true)", [])
    assert [[1, true]] == Postgrex.query!(pid, "SELECT * FROM foo1", []).rows
  end

  test "inserts/retrieves int", state do
    pid = state[:xt]
    Postgrex.query!(pid, "INSERT INTO foo2 (_id, num) VALUES (2, 666)", [])
    assert [[2, 666]] == Postgrex.query!(pid, "SELECT * FROM foo2", []).rows
  end

  test "inserts/retrieves float", state do
    pid = state[:xt]
    Postgrex.query!(pid, "INSERT INTO foo3 (_id, num) VALUES (3, 666.666)", [])
    assert [[3, 666.666]] == Postgrex.query!(pid, "SELECT * FROM foo3", []).rows
  end

  test "inserts/retrieves text", state do
    pid = state[:xt]
    Postgrex.query!(pid, "INSERT INTO foo4 (_id, num) VALUES (4, 'test-foo')", [])
    assert [[4, "test-foo"]] == Postgrex.query!(pid, "SELECT * FROM foo4", []).rows
  end

  test "inserts/retrieves timestamp", state do
    pid = state[:xt]
    ts = DateTime.utc_now |> DateTime.to_unix(:second)
    Postgrex.query!(pid, "INSERT INTO foo5 (_id, ts) VALUES (5, #{ts})", [])
    assert [[5,ts]] == Postgrex.query!(pid, "SELECT * FROM foo5", []).rows
  end

  test "should be able to insert an array valued field", state do
    pid = state[:xt]
    Postgrex.query!(pid, "INSERT INTO foo6 (_id, bars) VALUES (1, ARRAY[1,2,3])", [])
    result = Postgrex.query!(pid, "SELECT * FROM foo6", [])
    assert [[1, [1, 2, 3]]] == result.rows
  end

  test "should be able to insert and read back a UUID", state do
    pid = state[:xt]
    uuid = UUID.uuid4()
    Postgrex.query!(pid, "INSERT INTO foo7 (_id, u) VALUES (1, '#{uuid}')", [])
    [[id, u]] = Postgrex.query!(pid, "SELECT * FROM foo7 where _id = 1", []).rows
    assert 1 == id
    assert uuid == u
  end

  test "should be able to insert a date and read it back", state do
    pid = state[:xt]
    Postgrex.query!(pid, "INSERT INTO foo8 (_id, dt) VALUES (1, DATE '2020-01-01')", [])
    [[id, dat]] = Postgrex.query!(pid, "SELECT * FROM foo8 where _id = 1", []).rows
    assert 1 == id
    assert ~D[2020-01-01] == dat
  end

  # test "should be able to insert a JSON field and read it back", state do
  #   pid = state[:xt]
  #   json_field = %{"foo" => "bar"}
  #   Postgrex.query!(pid, "INSERT INTO foo9 (_id, json) VALUES (1, '{\"foo\": \"bar\"}'::JSON)", [])
  #   result = Postgrex.query!(pid, "SELECT * FROM foo9", [])
  #   assert [[1, %{"foo" => "bar"}]] == result.rows
  # end

end
