defmodule Clickhousex.QueryTest do
  @moduledoc false
  use ClickhouseCase, async: true

  alias Clickhousex.Result

  test "simple select", ctx do
    create_statement = """
    CREATE TABLE IF NOT EXISTS {{table}} (
      name String
    ) ENGINE = Memory
    """

    schema(ctx, create_statement)

    assert {:ok, _, %Result{command: :updated, num_rows: 1}} =
             insert(ctx, "INSERT INTO {{table}} VALUES ('qwerty')", [])

    assert {:ok, _,
            %Result{command: :selected, columns: ["name"], num_rows: 1, rows: [{"qwerty"}]}} =
             select_all(ctx)
  end

  test "parametrized queries", ctx do
    create_statement = """
    CREATE TABLE {{table}} (
      id Int32,
      name String
     ) ENGINE = Memory
    """

    assert {:ok, _, %Result{}} = schema(ctx, create_statement)

    assert {:ok, _, %Result{command: :updated, num_rows: 1}} =
             insert(ctx, "INSERT INTO {{table}} VALUES (?, ?)", [
               1,
               "abyrvalg"
             ])

    assert {:ok, _,
            %Result{
              command: :selected,
              columns: ["id", "name"],
              num_rows: 1,
              rows: [{1, "abyrvalg"}]
            }} = select_all(ctx)
  end

  test "scalar db types", ctx do
    create_statement = """
    CREATE TABLE {{table}} (
      u64_val UInt64,
      u32_val UInt32,
      u16_val UInt16,
      u8_val  UInt8,

      i64_val Int64,
      i32_val Int32,
      i16_val Int16,
      i8_val  Int8,

      f64_val Float64,
      f32_val Float32,

      string_val String,
      fixed_string_val FixedString(5),

      date_val Date,
      date_time_val DateTime
    )

    ENGINE = Memory
    """

    assert {:ok, _, %Result{}} = schema(ctx, create_statement)

    date = Date.utc_today()
    datetime = DateTime.utc_now()

    assert {:ok, _, %Result{command: :updated, num_rows: 1}} =
             insert(
               ctx,
               "INSERT INTO {{table}} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
               [
                 329,
                 328,
                 327,
                 32,
                 429,
                 428,
                 427,
                 42,
                 29.8,
                 4.0,
                 "This is long",
                 "hello",
                 date,
                 datetime
               ]
             )

    assert {:ok, _, %Result{columns: column_names, rows: [row]}} = select_all(ctx)

    naive_datetime =
      datetime
      |> DateTime.to_naive()
      |> NaiveDateTime.truncate(:second)

    assert row ==
             {329, 328, 327, 32, 429, 428, 427, 42, 29.8, 4.0, "This is long", "hello", date,
              naive_datetime}
  end

  test "nullables", ctx do
    create_statement = """
    CREATE TABLE {{table}} (
      id UInt64,
      u64_val Nullable(UInt64),
      string_val Nullable(String),
      date_val Nullable(Date),
      date_time_val Nullable(DateTime)
    ) ENGINE = Memory
    """

    now_date = Date.utc_today()
    now_datetime = DateTime.utc_now()

    assert {:ok, _, %Result{}} = schema(ctx, create_statement)

    assert {:ok, _, %Result{command: :updated, num_rows: 1}} =
             insert(
               ctx,
               "INSERT INTO {{table}} VALUES (?, ?, ?, ?, ?)",
               [1, 2, "hi", now_date, now_datetime]
             )

    assert {:ok, _, %Result{command: :updated, num_rows: 1}} =
             insert(
               ctx,
               "INSERT INTO {{table}} VALUES (?, ?, ?, ?, ?)",
               [2, nil, nil, nil, nil]
             )

    assert {:ok, _, %Result{rows: [row_1, row_2]}} = select_all(ctx)
  end

  test "arrays", ctx do
    create_statement = """
    CREATE TABLE {{table}} (
      id UInt64,
      arr_val Array(UInt64)
    ) ENGINE = Memory

    """

    assert {:ok, _, %Result{}} = schema(ctx, create_statement)

    assert {:ok, _, %Result{command: :updated, num_rows: 1}} =
             insert(ctx, "INSERT INTO {{table}} VALUES (?, ?)", [
               1,
               [1, 2, 3]
             ])

    assert {:ok, _, %Result{rows: [row]}} = select_all(ctx)

    assert row == {1, [1, 2, 3]}
  end

  test "arrays of a nullable type", ctx do
    create_statement = """
    CREATE TABLE {{table}} (
     id UInt64,
     nullable_value Array(Nullable(UInt64))
    ) Engine = Memory
    """

    assert {:ok, _, %Result{}} = schema(ctx, create_statement)

    assert {:ok, _, %Result{command: :updated, num_rows: 1}} =
             insert(ctx, "INSERT INTO {{table}} VALUES (?, ?)", [1, [1, nil, 2, nil]])

    assert {:ok, _, %Result{rows: [row]}} = select_all(ctx)
    assert row == {1, [1, nil, 2, nil]}
  end

  test "nested", ctx do
    create_statement = """
    CREATE TABLE {{table}} (
    id UInt64,
    fields Nested (
      label String,
      count UInt64
      )
    ) Engine = Memory
    """

    assert {:ok, _, %Result{}} = schema(ctx, create_statement)

    assert {:ok, _, %Result{command: :updated, num_rows: 1}} =
             insert(
               ctx,
               "INSERT INTO {{table}} (id, fields.label, fields.count) VALUES (?, ?, ?)",
               [
                 32,
                 ["label_1", "label_2", "label_3"],
                 [6, 9, 42]
               ]
             )

    assert {:ok, _, %Result{rows: [row]}} = select_all(ctx)
    assert row == {32, ~w(label_1 label_2 label_3), [6, 9, 42]}

    assert {:ok, _, %Result{rows: [label_1, label_2, label_3]}} =
             select(ctx, "SELECT * from {{table}} ARRAY JOIN fields where id = 32", [])

    assert {32, "label_1", 6} == label_1
    assert {32, "label_2", 9} == label_2
    assert {32, "label_3", 42} == label_3
  end

  test "queries that insert more than one row", ctx do
    create_statement = """
    CREATE TABLE {{table}} (
      id Int32,
      name String
    ) ENGINE = Memory
    """

    assert {:ok, _, %Result{}} = schema(ctx, create_statement)

    assert {:ok, _, %Result{command: :updated, num_rows: 1}} =
             insert(ctx, "INSERT INTO {{table}} VALUES (?, ?)", [1, "abyrvalg"])

    insert(ctx, "INSERT INTO {{table}} VALUES (?, ?)", [2, "stinky"])

    assert {:ok, _,
            %Result{
              command: :selected,
              columns: ["id", "name"],
              num_rows: 2,
              rows: [{1, "abyrvalg"}, {2, "stinky"}]
            }} = select_all(ctx)
  end

  test "selecting specific fields", ctx do
    create_statement = """
    CREATE TABLE {{table}} (
      id Int64,
      name String,
      email String
    ) ENGINE = Memory
    """

    assert {:ok, _, %Result{}} = schema(ctx, create_statement)

    assert {:ok, _, %{command: :updated, num_rows: 1}} =
             insert(ctx, "INSERT INTO {{table}} VALUES (?, ?, ?)", [1, "foobie", "foo@bar.com"])

    assert {:ok, _, %{command: :updated, num_rows: 1}} =
             insert(ctx, "INSERT INTO {{table}} VALUES (?, ?, ?)", [2, "barbie", "bar@bar.com"])

    assert {:ok, _, %{rows: [row]}} = select(ctx, "SELECT email FROM {{table}} WHERE id = ?", [1])
    assert row == {"foo@bar.com"}
  end

  test "selecting with in", ctx do
    create_statement = """
    CREATE TABLE {{table}} (
      id Int64,
      name String,
      email String
    ) ENGINE = Memory
    """

    assert {:ok, _, %Result{}} = schema(ctx, create_statement)

    assert {:ok, _, %{command: :updated, num_rows: 1}} =
             insert(ctx, "INSERT INTO {{table}} VALUES (?, ?, ?)", [1, "foobie", "foo@bar.com"])

    assert {:ok, _, %{command: :updated, num_rows: 1}} =
             insert(ctx, "INSERT INTO {{table}} VALUES (?, ?, ?)", [2, "barbie", "bar@bar.com"])

    assert {:ok, _, %{rows: [{"foo@bar.com"}, {"bar@bar.com"}]}} =
             select(ctx, "SELECT email FROM {{table}} WHERE id IN (?)", [[1, 2]])
  end

  test "updating rows via alter", ctx do
    create_statement = """
    CREATE TABLE {{table}} (
      id Int64,
      name String,
      email String
    ) ENGINE = MergeTree
    PARTITION BY id
    ORDER BY id SETTINGS index_granularity = 8192
    """

    assert {:ok, _, %Result{}} = schema(ctx, create_statement)

    assert {:ok, _, %{command: :updated, num_rows: 1}} =
             insert(ctx, "INSERT INTO {{table}} VALUES (?, ?, ?)", [1, "foobie", "foo@bar.com"])

    assert {:ok, _, %{command: :updated, num_rows: 1}} =
             insert(ctx, "INSERT INTO {{table}} VALUES (?, ?, ?)", [2, "barbie", "bar@bar.com"])

    assert {:ok, _, _} =
             select(ctx, "ALTER TABLE {{table}} UPDATE email = ? WHERE id = ?", [
               "foobar@bar.com",
               1
             ])
  end
end
