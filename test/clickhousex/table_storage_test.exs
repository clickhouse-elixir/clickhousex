defmodule Clickhousex.TableStorageTest do
  use ExUnit.Case, async: true

  alias Clickhousex.Result

  setup_all do
    {:ok, client} = Clickhousex.start_link([])
    {:ok, client: client}
  end

  setup %{client: client} do
    on_exit(fn ->
      Clickhousex.query(client, "DROP DATABASE table_storage_test", [])
    end)

    Clickhousex.query(client, "CREATE DATABASE table_storage_test", [])
    {:ok, [client: client]}
  end

  test "can create and drop table", %{client: client} do
    assert {:ok, _, %Result{}} =
             Clickhousex.query(
               client,
               "CREATE TABLE table_storage_test.can_create_drop(id Int32) ENGINE = Memory",
               []
             )

    assert {:ok, _, %Result{}} =
             Clickhousex.query(client, "DROP TABLE table_storage_test.can_create_drop", [])
  end

  test "returns correct error when dropping table that doesn't exist", %{client: client} do
    assert {:error, %{code: :base_table_or_view_not_found}} =
             Clickhousex.query(client, "DROP TABLE table_storage_test.not_exist", [])
  end

  test "returns correct error when creating a table that already exists", %{client: client} do
    Clickhousex.query(client, "DROP TABLE table_storage_test.table_already_exists", [])

    sql = "CREATE TABLE table_storage_test.table_already_exists(id Int32) ENGINE = Memory"
    assert {:ok, _, %Result{}} = Clickhousex.query(client, sql, [])
    assert {:error, %{code: :table_already_exists}} = Clickhousex.query(client, sql, [])
  end
end
