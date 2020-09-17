defmodule Clickhousex.Codec.RowBinary.Old do
  @moduledoc false

  alias Clickhousex.{Codec, Codec.Binary}

  require Record

  Record.defrecord(:state, column_count: 0, column_names: [], column_types: [], rows: [], count: 0)

  @behaviour Codec

  @impl Codec
  def response_format do
    "RowBinaryWithNamesAndTypes"
  end

  @impl Codec
  def request_format do
    "Values"
  end

  @impl Codec
  def encode(query, replacements, params) do
    params =
      Enum.map(params, fn
        %DateTime{} = dt -> DateTime.to_unix(dt)
        other -> other
      end)

    Codec.Values.encode(query, replacements, params)
  end

  @impl Codec
  def new do
    nil
  end

  @impl Codec
  def append(nil, data) do
    case Binary.decode(data, :varint) do
      {:ok, column_count, rest} ->
        state = state(column_count: column_count)

        case decode_column_names(rest, column_count, state) do
          state() = state -> state
          {:resume, _} = resumer -> resumer
        end

      {:resume, _} ->
        {:resume, &append(nil, data <> &1)}
    end
  end

  def append(state() = state, data) do
    decode_rows(data, state)
  end

  def append({:resume, resumer}, data) do
    case resumer.(data) do
      {:resume, _} = resumer -> resumer
      state() = state -> state
    end
  end

  @impl Codec
  def decode(state(column_names: column_names, rows: rows, count: count)) do
    {:ok, %{column_names: column_names, rows: Enum.reverse(rows), count: count}}
  end

  def decode(nil) do
    decode(state())
  end

  defp decode_column_names(
         bytes,
         0,
         state(column_names: names, column_count: column_count) = state
       ) do
    decode_column_types(bytes, column_count, state(state, column_names: Enum.reverse(names)))
  end

  defp decode_column_names(bytes, remaining_columns, state(column_names: names) = state) do
    case Binary.decode(bytes, :string) do
      {:ok, column_name, rest} ->
        decode_column_names(
          rest,
          remaining_columns - 1,
          state(state, column_names: [column_name | names])
        )

      {:resume, _} ->
        {:resume, fn more_data -> decode_column_names(bytes <> more_data, remaining_columns, state) end}
    end
  end

  defp decode_column_types(bytes, 0, state(column_types: types) = state) do
    decode_rows(bytes, state(state, column_types: Enum.reverse(types)))
  end

  defp decode_column_types(bytes, remaining_columns, state(column_types: types) = state) do
    case Binary.decode(bytes, :string) do
      {:ok, column_type, rest} ->
        column_type = parse_type(column_type)
        column_types = [column_type | types]

        decode_column_types(rest, remaining_columns - 1, state(state, column_types: column_types))

      {:resume, _} ->
        {:resume, fn more_data -> decode_column_types(bytes <> more_data, remaining_columns, state) end}
    end
  end

  defp decode_rows(<<>>, state() = state) do
    state
  end

  defp decode_rows(bytes, state(column_types: column_types, rows: rows, count: count) = state) do
    case decode_row(bytes, column_types, []) do
      {:ok, row, rest} ->
        decode_rows(rest, state(state, rows: [row | rows], count: count + 1))

      {:resume, _} ->
        {:resume, fn more_data -> decode_rows(bytes <> more_data, state) end}
    end
  end

  defp decode_row(<<bytes::bits>>, [], row) do
    row_tuple =
      row
      |> Enum.reverse()
      |> List.to_tuple()

    {:ok, row_tuple, bytes}
  end

  defp decode_row(<<1, rest::binary>>, [{:nullable, _} | types], row) do
    decode_row(rest, types, [nil | row])
  end

  defp decode_row(<<0, rest::binary>>, [{:nullable, actual_type} | types], row) do
    decode_row(rest, [actual_type | types], row)
  end

  defp decode_row(<<bytes::bits>>, [{:fixed_string, length} | types], row)
       when byte_size(bytes) >= length do
    <<value::binary-size(length), rest::binary>> = bytes
    decode_row(rest, types, [value | row])
  end

  defp decode_row(<<bytes::bits>>, [{:fixed_string, _} | _] = current_types, row) do
    {:resume, fn more_data -> decode_row(bytes <> more_data, current_types, row) end}
  end

  defp decode_row(<<bytes::bits>>, [{:array, elem_type} | types] = current_types, row) do
    case Binary.decode(bytes, {:list, elem_type}) do
      {:ok, value, rest} ->
        decode_row(rest, types, [value | row])

      {:resume, _} ->
        {:resume, fn more_data -> decode_row(bytes <> more_data, current_types, row) end}
    end
  end

  defp decode_row(<<bytes::bits>>, [type | types] = current_types, row) do
    case Binary.decode(bytes, type) do
      {:ok, value, rest} ->
        decode_row(rest, types, [value | row])

      {:resume, _} ->
        {:resume, fn more_data -> decode_row(bytes <> more_data, current_types, row) end}
    end
  end

  defp parse_type(<<"Nullable(", type::binary>>) do
    rest_type =
      type
      |> String.replace_suffix(")", "")
      |> parse_type()

    {:nullable, rest_type}
  end

  defp parse_type(<<"FixedString(", rest::binary>>) do
    case Integer.parse(rest) do
      {length, ")"} ->
        {:fixed_string, length}

      {_length, rest} ->
        raise "Expected ')', but got '#{rest}'"
    end
  end

  defp parse_type(<<"Array(", type::binary>>) do
    rest_type =
      type
      |> String.replace_suffix(")", "")
      |> parse_type()

    {:array, rest_type}
  end

  @clickhouse_mappings [
    {"Int64", :i64},
    {"Int32", :i32},
    {"Int16", :i16},
    {"Int8", :i8},
    {"UInt64", :u64},
    {"UInt32", :u32},
    {"UInt16", :u16},
    {"UInt8", :u8},
    {"Float64", :f64},
    {"Float32", :f32},
    {"Float16", :f16},
    {"Float8", :f8},
    {"String", :string},
    {"Date", :date},
    {"DateTime", :datetime}
  ]
  for {clickhouse_type, local_type} <- @clickhouse_mappings do
    defp parse_type(unquote(clickhouse_type)) do
      unquote(local_type)
    end
  end
end
