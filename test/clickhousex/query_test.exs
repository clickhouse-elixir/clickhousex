defmodule Clickhousex.QueryTest do
  @moduledoc false
  use ClickhouseCase, async: true

  alias Clickhousex.Query
  alias Clickhousex.Result

  test "materialize view create query", ctx do
    create_statement = """
    CREATE TABLE IF NOT EXISTS {{table}} (
      name String
    ) ENGINE = Memory
    """

    schema(ctx, create_statement)

    assert {:ok, %Query{type: :create}, _result} =
             schema(ctx, """
             CREATE MATERIALIZED VIEW IF NOT EXISTS material_view
             ENGINE = MergeTree() ORDER BY name
             AS SELECT
               name
             FROM {{table}}
             """)
  end

  test "simple select", ctx do
    create_statement = """
    CREATE TABLE IF NOT EXISTS {{table}} (
      name String
    ) ENGINE = Memory
    """

    schema(ctx, create_statement)

    assert {:ok, _, %Result{command: :updated, num_rows: 1}} =
             insert(ctx, "INSERT INTO {{table}} VALUES ('qwerty')", [])

    assert {:ok, _, %Result{command: :selected, columns: ["name"], num_rows: 1, rows: [{"qwerty"}]}} = select_all(ctx)
  end

  describe "parameterized queries" do
    test "parametrized insert queries", ctx do
      create_statement = """
      CREATE TABLE {{table}} (
        id Int32,
        number UInt64,
        float Float64,
        name String
       ) ENGINE = Memory
      """

      assert {:ok, _, %Result{}} = schema(ctx, create_statement)

      assert {:ok, _, %Result{command: :updated, num_rows: 1}} =
               insert(ctx, "INSERT INTO {{table}} VALUES (?, ?, ?, ?)", [
                 1,
                 643_225,
                 54356.0,
                 "abyrvalg"
               ])

      assert {:ok, _,
              %Result{
                command: :selected,
                columns: ["id", "number", "float", "name"],
                num_rows: 1,
                rows: [{1, 643_225, 54356.0, "abyrvalg"}]
              }} = select_all(ctx)
    end

    test "parameterized select queries", ctx do
      create_statement = """
      CREATE TABLE {{table}} (
        id Int32,
        number UInt64,
        float Float64,
        name String
       ) ENGINE = Memory
      """

      assert {:ok, _, %Result{}} = schema(ctx, create_statement)

      insert_query = "INSERT INTO {{table}} VALUES (?, ?, ?, ?), (?, ?, ?, ?), (?, ?, ?, ?)"

      assert {:ok, _, %Result{command: :updated, num_rows: 1}} =
               insert(ctx, insert_query, [
                 1,
                 643_225,
                 54356.0,
                 "abyrvalg",
                 2,
                 2,
                 2.0,
                 "B",
                 3,
                 3,
                 3.14,
                 "C"
               ])

      expected_result = %Result{
        command: :selected,
        columns: ["id", "number", "float", "name"],
        num_rows: 1,
        rows: [{3, 3, 3.14, "C"}]
      }

      query = "SELECT * FROM {{table}} WHERE id = ?"
      assert {:ok, _, ^expected_result} = select(ctx, query, [3])

      query = "SELECT * FROM {{table}} WHERE number = ?"
      assert {:ok, _, ^expected_result} = select(ctx, query, [3.0])

      query = "SELECT * FROM {{table}} WHERE float >= ? AND float <= ?"
      assert {:ok, _, ^expected_result} = select(ctx, query, [3.13, 3.15])

      query = "SELECT * FROM {{table}} WHERE name = ?"
      assert {:ok, _, ^expected_result} = select(ctx, query, ["C"])

      # Check for correct error handling on length
      query = "SELECT * FROM {{table}} WHERE name = ?"

      assert_raise ArgumentError,
                   "The number of parameters does not correspond to the number of question marks!",
                   fn -> select(ctx, query, []) end

      query = "SELECT * FROM {{table}} WHERE name = ?"

      assert_raise ArgumentError,
                   "The number of parameters does not correspond to the number of question marks!",
                   fn -> select(ctx, query, ["C", "D"]) end
    end

    test "parameterized date queries", ctx do
      create_statement = """
      CREATE TABLE {{table}} (
        id Int32,
        date1 Date,
        datetime1 DateTime,
        datetime2 DateTime64,
        datetime3 DateTime64(0),
        datetime4 DateTime64(6)
       ) ENGINE = Memory
      """

      assert {:ok, _, %Result{}} = schema(ctx, create_statement)

      insert_query = """
      INSERT INTO {{table}} VALUES
      (?, ?, ?, ?, ?, ?),
      (?, ?, ?, ?, ?, ?),
      (?, ?, ?, ?, ?, ?)
      """

      assert {:ok, _, %Result{command: :updated, num_rows: 1}} =
               insert(ctx, insert_query, [
                 1,
                 ~D[1987-11-05],
                 ~N[1987-11-05T13:55:14.123],
                 "1987-11-05T13:55:14.123",
                 "1987-11-05T13:55:14",
                 "1987-11-05T13:55:14.123456",
                 2,
                 ~D[2087-11-05],
                 ~N[2087-11-05T13:55:14.123],
                 "2087-11-05T13:55:14.123",
                 "2087-11-05T13:55:14",
                 "2087-11-05T13:55:14.123456",
                 3,
                 ~D[2087-11-06],
                 ~N[2087-11-06T13:55:14.123],
                 "2087-11-06T13:55:14.123",
                 "2087-11-06T13:55:14",
                 "2087-11-05T13:55:14.000000"
               ])

      expected_result = %Result{
        command: :selected,
        columns: ["id", "date1", "datetime1", "datetime2", "datetime3", "datetime4"],
        num_rows: 1,
        rows: [
          {2, ~D[2087-11-05], ~N[2087-11-05T13:55:14], ~N[2087-11-05T13:55:14.123000], ~N[2087-11-05T13:55:14.000000],
           ~N[2087-11-05T13:55:14.123456]}
        ]
      }

      query = "SELECT * FROM {{table}} WHERE date1 = ?"
      assert {:ok, _, ^expected_result} = select(ctx, query, [~D[2087-11-05]])

      query = "SELECT * FROM {{table}} WHERE datetime1 = ?"
      assert {:ok, _, ^expected_result} = select(ctx, query, [~N[2087-11-05T13:55:14.123]])

      query = "SELECT * FROM {{table}} WHERE datetime2 = ?"
      assert {:ok, _, ^expected_result} = select(ctx, query, [~N[2087-11-05T13:55:14.123]], datetime_precision: :dt64)

      query = "SELECT * FROM {{table}} WHERE datetime3 = ?"
      assert {:ok, _, ^expected_result} = select(ctx, query, [~N[2087-11-05T13:55:14.123]], datetime_precision: 0)

      query = "SELECT * FROM {{table}} WHERE datetime4 = ?"
      assert {:ok, _, ^expected_result} = select(ctx, query, [~U[2087-11-05T13:55:14.123456Z]], datetime_precision: 6)

      query = "SELECT * FROM {{table}} WHERE datetime4 = ?"
      {:ok, _, actual_result} = select(ctx, query, [~N[2087-11-05T13:55:14.123456]], datetime_precision: 0)
      assert expected_result != actual_result
    end
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

    assert {:ok, _, %Result{columns: _column_names, rows: [row]}} = select_all(ctx)

    naive_datetime =
      datetime
      |> DateTime.to_naive()
      |> NaiveDateTime.truncate(:second)

    assert row ==
             {329, 328, 327, 32, 429, 428, 427, 42, 29.8, 4.0, "This is long", "hello", date, naive_datetime}
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

    assert {:ok, _, %Result{rows: rows}} = select_all(ctx)
    [row_1, row_2] = Enum.sort(rows, fn row_1, row_2 -> elem(row_1, 1) <= elem(row_2, 1) end)
    assert {1, 2, "hi", _, _} = row_1
    assert row_2 == {2, nil, nil, nil, nil}
  end

  test "arrays", ctx do
    create_statement = """
    CREATE TABLE {{table}} (
      id UInt64,
      arr_val Array(UInt64),
      nullable_val Array(Nullable(String))
    ) ENGINE = Memory

    """

    assert {:ok, _, %Result{}} = schema(ctx, create_statement)

    assert {:ok, _, %Result{command: :updated, num_rows: 1}} =
             insert(ctx, "INSERT INTO {{table}} VALUES (?, ?, ?)", [
               1,
               [1, 2, 3],
               ["hi", nil, "dude"]
             ])

    assert {:ok, _, %Result{rows: [row]}} = select_all(ctx)

    assert row == {1, [1, 2, 3], ["hi", nil, "dude"]}
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
              rows: rows
            }} = select_all(ctx)

    assert {1, "abyrvalg"} in rows
    assert {2, "stinky"} in rows
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

    assert {:ok, _, %{rows: rows}} = select(ctx, "SELECT email FROM {{table}} WHERE id IN (?)", [[1, 2]])

    assert [{"bar@bar.com"}, {"foo@bar.com"}] == Enum.sort(rows)
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

  test "long column names", ctx do
    long_column_name =
      "tSDco1R4Uw3vMlH04XWsEFbtDHJjx492DHBDFx3gQWYMNtq6qs5rh" <>
        "H1g6K3th2x5YaQ1vVMZ6Ub59KMsM8cWsxVgwHJoKQgzZB6Vqyw" <>
        "kIw8fZXyBB4WdDqIEUSVYsvAtDsPM1BMZcLzXmTCdvt1KUX"

    select_statement = """
    SELECT 1 AS
    `#{long_column_name}`
    """

    assert {:ok, _, %{columns: columns}} = select(ctx, select_statement, [])
    assert [long_column_name] == columns
  end
end
