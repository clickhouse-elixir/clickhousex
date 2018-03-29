defmodule Clickhousex.StorageTest do
  use ExUnit.Case, async: true

  alias Clickhousex.Result

  setup do
    {:ok, pid} = Clickhousex.start_link([])
    Clickhousex.query(pid, "DROP DATABASE storage_test", [])
    {:ok, [pid: pid]}
  end

  test "can create and drop database", %{pid: pid} do
    assert {:ok, _, %Result{}} = Clickhousex.query(pid, "CREATE DATABASE storage_test", [])
    assert {:ok, _, %Result{}} = Clickhousex.query(pid, "DROP DATABASE storage_test", [])
  end

  test "returns correct error when dropping database that doesn't exist", %{pid: pid} do
    assert {:error, %{code: :database_does_not_exists}} = Clickhousex.query(pid, "DROP DATABASE storage_test", [])
  end

  test "returns correct error when creating a database that already exists", %{pid: pid} do
    assert {:ok, _, %Result{}} = Clickhousex.query(pid, "CREATE DATABASE storage_test", [])
    assert {:error, %{code: :database_already_exists}} = Clickhousex.query(pid, "CREATE DATABASE storage_test", [])
  end
end
