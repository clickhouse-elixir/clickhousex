defmodule Clickhousex.StorageTest do
  use ExUnit.Case, async: true

  alias Clickhousex.Result

  setup_all do
    {:ok, client} = Clickhousex.start_link([])
    {:ok, client: client}
  end

  setup %{client: client} do
    on_exit(fn ->
      Clickhousex.query(client, "DROP DATABASE storage_test", [])
    end)

    {:ok, [client: client]}
  end

  test "can create and drop database", %{client: client} do
    assert {:ok, _, %Result{}} = Clickhousex.query(client, "CREATE DATABASE storage_test", [])
    assert {:ok, _, %Result{}} = Clickhousex.query(client, "DROP DATABASE storage_test", [])
  end

  test "returns correct error when dropping database that doesn't exist", %{client: client} do
    assert {:error, %{code: :database_does_not_exists}} =
             Clickhousex.query(client, "DROP DATABASE storage_test", [])
  end

  test "returns correct error when creating a database that already exists", %{client: client} do
    assert {:ok, _, %Result{}} = Clickhousex.query(client, "CREATE DATABASE storage_test", [])

    assert {:error, %{code: :database_already_exists}} =
             Clickhousex.query(client, "CREATE DATABASE storage_test", [])
  end
end
