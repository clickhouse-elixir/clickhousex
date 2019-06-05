defmodule Clickhousex.Codec.RowBinary do
  alias Clickhousex.{Codec, Codec.Binary}

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

    Clickhousex.Codec.Values.encode(query, replacements, params)
  end

  @impl Codec
  def decode(response) when is_binary(response) do
    {:ok, column_count, rest} = Binary.decode(response, :varint)
    decode_metadata(rest, column_count)
  end

  defp decode_metadata(bytes, column_count) do
    {:ok, column_names, rest} = decode_column_names(bytes, column_count, [])
    {:ok, column_types, rest} = decode_column_types(rest, column_count, [])

    {:ok, rows} = decode_rows(rest, column_types, [])
    {:ok, %{column_names: column_names, rows: rows, count: 0}}
  end

  defp decode_column_names(bytes, 0, names) do
    {:ok, Enum.reverse(names), bytes}
  end

  defp decode_column_names(bytes, column_count, names) do
    {:ok, column_name, rest} = Binary.decode(bytes, :string)
    decode_column_names(rest, column_count - 1, [column_name | names])
  end

  defp decode_column_types(bytes, 0, types) do
    {:ok, Enum.reverse(types), bytes}
  end

  defp decode_column_types(bytes, column_count, types) do
    {:ok, column_type, rest} = Binary.decode(bytes, :string)
    decode_column_types(rest, column_count - 1, [to_type(column_type) | types])
  end

  defp decode_rows(<<>>, _, rows) do
    {:ok, Enum.reverse(rows)}
  end

  defp decode_rows(bytes, atom_types, rows) do
    {:ok, row, rest} = decode_row(bytes, atom_types, [])

    decode_rows(rest, atom_types, [row | rows])
  end

  defp decode_row(bytes, [], row) do
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

  defp decode_row(bytes, [{:fixed_string, length} | types], row) do
    <<value::binary-size(length), rest::binary>> = bytes
    decode_row(rest, types, [value | row])
  end

  defp decode_row(bytes, [{:array, elem_type} | types], row) do
    {:ok, value, rest} = Binary.decode(bytes, {:list, elem_type})
    decode_row(rest, types, [value | row])
  end

  defp decode_row(bytes, [type | types], row) do
    {:ok, value, rest} = Binary.decode(bytes, type)
    decode_row(rest, types, [value | row])
  end

  defp to_type(<<"Nullable(", type::binary>>) do
    rest_type =
      type
      |> String.replace_suffix(")", "")
      |> to_type()

    {:nullable, rest_type}
  end

  defp to_type(<<"FixedString(", rest::binary>>) do
    case Integer.parse(rest) do
      {length, rest} ->
        rest
        |> String.replace_suffix(")", "")

        {:fixed_string, length}
    end
  end

  defp to_type(<<"Array(", type::binary>>) do
    rest_type =
      type
      |> String.replace_suffix(")", "")
      |> to_type()

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
    defp to_type(unquote(clickhouse_type)) do
      unquote(local_type)
    end
  end
end
