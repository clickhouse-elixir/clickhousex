defmodule Clickhousex.TableStorageTest do
  use ClickhouseCase, async: true

  alias Clickhousex.Result

  test "can create and drop table", ctx do
    create_statement = """
    CREATE TABLE {{table}} (id Int32) ENGINE = Memory
    """

    assert {:ok, %Result{}} = schema(ctx, create_statement)

    assert {:ok, %Result{}} = schema(ctx, "DROP TABLE {{ table }}")
  end

  test "returns correct error when dropping table that doesn't exist", ctx do
    assert {:error, %{code: :base_table_or_view_not_found}} =
             schema(ctx, "DROP TABLE table_storage_test.not_exist")
  end

  test "returns correct error when creating a table that already exists", ctx do
    create_statement = """
    CREATE TABLE {{ table }}
    (id Int32) ENGINE = Memory
    """

    assert {:ok, %Result{}} = schema(ctx, create_statement)
    assert {:error, %{code: :table_already_exists}} = schema(ctx, create_statement)
  end
end
