defmodule Clickhousex.StorageTest do
  use ClickhouseCase, async: true

  alias Clickhousex.Result

  test "can create and drop database", ctx do
    assert {:ok, _, %Result{}} = schema(ctx, "CREATE DATABASE other_db")
    assert {:ok, _, %Result{}} = schema(ctx, "DROP DATABASE other_db")
  end

  test "returns correct error when dropping database that doesn't exist", ctx do
    assert {:error, %{code: :database_does_not_exists}} = schema(ctx, "DROP DATABASE random_db ")
  end

  test "returns correct error when creating a database that already exists", ctx do
    assert {:error, %{code: :database_already_exists}} =
             schema(ctx, "CREATE DATABASE {{database}}")
  end
end
