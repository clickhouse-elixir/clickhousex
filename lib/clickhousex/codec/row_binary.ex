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
  def encode(query, params) do
    params =
      Enum.map(params, fn
        %DateTime{} = dt -> DateTime.to_unix(dt)
        other -> other
      end)

    Clickhousex.Codec.Values.encode(query, params)
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

  defp decode_row(<<1, rest::binary>>, [[:nullable, _] | types], row) do
    decode_row(rest, types, [nil | row])
  end

  defp decode_row(<<0, rest::binary>>, [[:nullable, actual_type] | types], row) do
    decode_row(rest, [[actual_type] | types], row)
  end

  defp decode_row(bytes, [[:fixed_string, length] | types], row) do
    <<value::binary-size(length), rest::binary>> = bytes
    decode_row(rest, types, [value | row])
  end

  defp decode_row(bytes, [[:array, elem_type] | types], row) do
    {:ok, value, rest} = Binary.decode(bytes, {:list, elem_type})
    decode_row(rest, types, [value | row])
  end

  defp decode_row(bytes, [[type] | types], row) do
    {:ok, value, rest} = Binary.decode(bytes, type)
    decode_row(rest, types, [value | row])
  end

  defp to_type(val) do
    val
    |> to_type([])
    |> Enum.reverse()
  end

  defp to_type(<<"Nullable(", type::binary>>, acc) do
    to_type(String.replace_suffix(type, ")", ""), [:nullable | acc])
  end

  defp to_type(<<"FixedString(", rest::binary>>, acc) do
    case Integer.parse(rest) do
      {length, rest} ->
        rest
        |> String.replace_suffix(")", "")
        |> to_type([length, :fixed_string | acc])
    end
  end

  defp to_type(<<"Array(", type::binary>>, acc) do
    to_type(String.replace_suffix(type, ")", ""), [:array | acc])
  end

  defp to_type(raw_type, acc) do
    case raw_type do
      "Int64" -> [:i64 | acc]
      "Int32" -> [:i32 | acc]
      "Int16" -> [:i16 | acc]
      "Int8" -> [:i8 | acc]
      "UInt64" -> [:u64 | acc]
      "UInt32" -> [:u32 | acc]
      "UInt16" -> [:u16 | acc]
      "UInt8" -> [:u8 | acc]
      "Float64" -> [:f64 | acc]
      "Float32" -> [:f32 | acc]
      "Float16" -> [:f16 | acc]
      "Float8" -> [:f8 | acc]
      "String" -> [:string | acc]
      "Date" -> [:date | acc]
      "DateTime" -> [:datetime | acc]
      "" -> acc
    end
  end
end
