database = "clickhousex"
table = "#{database}.benchmarks"
create_database = "CREATE DATABASE IF NOT EXISTS #{database}"
drop_database = "DROP DATABASE #{database}"

create_table = """
CREATE TABLE IF NOT EXISTS #{table} (
  u64_val UInt64,
  string_val String,
  list_val Array(String),
  nullable_u64_val Nullable(UInt64),
  date_val Date,
  datetime_val DateTime
) ENGINE = Memory
"""

alias Clickhousex, as: CH

{:ok, client} = CH.start_link()
{:ok, _, _} = CH.query(client, create_database, [])
{:ok, _, _} = CH.query(client, create_table, [])

insert = fn column_name, value ->
  {:ok, _, _} = CH.query(client, "INSERT INTO #{table} (#{column_name}) VALUES (?)", [value])
end

select = fn column_name, value ->
  {:ok, _, result} =
    CH.query(client, "SELECT #{column_name} FROM #{table} WHERE #{column_name} = ?", [value])
end

seed_data_count = 1000

l = Enum.map(1..50, fn n -> String.duplicate("#{n}", 5) end)
date = Date.utc_today()
date_time = DateTime.utc_now()

for n <- 1..seed_data_count do
  insert.("u64_val", n)
end

for string <- l do
  insert.("string_val", string)
end

Benchee.run(%{
  "Insert ints" => fn ->
    insert.("u64_val", 4_924_848_124_381)
  end,
  "Insert strings" => fn ->
    insert.("string_val", "This is a long string")
  end,
  "Insert lists" => fn ->
    insert.("list_val", ["Hello there guys"])
  end,
  "Insert nullable non-null" => fn ->
    insert.("nullable_u64_val", 4_928_481_949_828_321)
  end,
  "Insert nullable null" => fn ->
    insert.("nullable_u64_val", nil)
  end
  # "Insert date" => fn ->
  #   {:ok, _, _} = CH.query(client, "INSERT INTO #{table} (date_val) VALUES (?)", [date])
  # end,
  # "Insert datetime" => fn ->
  #   {:ok, _, _} = CH.query(client, "INSERT INTO #{table} (datetime_val) VALUES (?)", [date_time])
  # end
})

Benchee.run(%{
  "Select ints" => fn ->
    select.("u64_val", :rand.uniform(seed_data_count))
  end,
  "Select strings" => fn ->
    select.("string_val", "5050505050")
  end,
  "selecting all" => fn ->
    {:ok, _, _} = CH.query(client, "SELECT * from #{table}", [])
  end
})

{:ok, _, _} = CH.query(client, drop_database, [])
