defmodule Clickhousex.TableStorageTest do
  use ExUnit.Case, async: true

  alias Clickhousex.Result

  setup do
    {:ok, pid} = Clickhousex.start_link([])
    Clickhousex.query(pid, "CREATE DATABASE table_storage_test", [])
    {:ok, [pid: pid]}
  end

  test "can create and drop table", %{pid: pid} do
    assert {:ok, _, %Result{}}
           = Clickhousex.query(pid, "CREATE TABLE table_storage_test.can_create_drop(id Int32) ENGINE = Memory", [])
    assert {:ok, _, %Result{}}
           = Clickhousex.query(pid, "DROP TABLE table_storage_test.can_create_drop", [])
  end

  test "returns correct error when dropping table that doesn't exist", %{pid: pid} do
    assert {:error, %{code: :base_table_or_view_not_found}}
           = Clickhousex.query(pid, "DROP TABLE table_storage_test.not_exist", [])
  end

  test "returns correct error when creating a table that already exists", %{pid: pid} do
    Clickhousex.query(pid, "DROP TABLE table_storage_test.table_already_exists", [])

    sql = "CREATE TABLE table_storage_test.table_already_exists(id Int32) ENGINE = Memory"
    assert {:ok, _, %Result{}} = Clickhousex.query(pid, sql, [])
    assert {:error, %{code: :table_already_exists}} = Clickhousex.query(pid, sql, [])
  end
end
