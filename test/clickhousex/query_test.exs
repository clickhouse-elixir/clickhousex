defmodule Clickhousex.QueryTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Clickhousex.Result

  setup_all do
    {:ok, client} = Clickhousex.start_link([])
    {:ok, client: client}
  end

  setup %{client: client} do
    on_exit(fn ->
      Clickhousex.query!(client, "DROP DATABASE IF EXISTS query_test", [])
    end)

    {:ok, _, _} = Clickhousex.query(client, "CREATE DATABASE query_test", [])

    {:ok, [client: client]}
  end

  test "simple select", %{client: client} do
    assert {:ok, _, %Result{}} =
             Clickhousex.query(
               client,
               "CREATE TABLE IF NOT EXISTS query_test.simple_select (name String) ENGINE = Memory",
               []
             )

    assert {:ok, _, %Result{command: :updated, num_rows: 1}} =
             Clickhousex.query(
               client,
               ["INSERT INTO query_test.simple_select VALUES ('qwerty')"],
               []
             )

    assert {:ok, _,
            %Result{command: :selected, columns: ["name"], num_rows: 1, rows: [{"qwerty"}]}} =
             Clickhousex.query(client, "SELECT * FROM query_test.simple_select", [])
  end

  test "parametrized queries", %{client: client} do
    assert {:ok, _, %Result{}} =
             Clickhousex.query(
               client,
               "CREATE TABLE query_test.parametrized_query(id Int32, name String) ENGINE = Memory",
               []
             )

    assert {:ok, _, %Result{command: :updated, num_rows: 1}} =
             Clickhousex.query(
               client,
               ["INSERT INTO query_test.parametrized_query VALUES (?, ?)"],
               [
                 1,
                 "abyrvalg"
               ]
             )

    assert {:ok, _,
            %Result{
              command: :selected,
              columns: ["id", "name"],
              num_rows: 1,
              rows: [{1, "abyrvalg"}]
            }} = Clickhousex.query(client, "SELECT * FROM query_test.parametrized_query", [])
  end

  test "queries that insert more than one row", %{client: client} do
    assert {:ok, _, %Result{}} =
             Clickhousex.query(
               client,
               "CREATE TABLE query_test.parametrized_query(id Int32, name String) ENGINE = Memory",
               []
             )

    assert {:ok, _, %Result{command: :updated, num_rows: 1}} =
             Clickhousex.query(
               client,
               ["INSERT INTO query_test.parametrized_query VALUES (?, ?)"],
               [
                 1,
                 "abyrvalg"
               ]
             )

    Clickhousex.query(client, ["INSERT INTO query_test.parametrized_query VALUES (?, ?)"], [
      2,
      "stinky"
    ])

    assert {:ok, _,
            %Result{
              command: :selected,
              columns: ["id", "name"],
              num_rows: 2,
              rows: [{1, "abyrvalg"}, {2, "stinky"}]
            }} = Clickhousex.query(client, "SELECT * FROM query_test.parametrized_query", [])
  end
end
