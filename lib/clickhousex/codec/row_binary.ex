defmodule Clickhousex.Codec.RowBinary do
  @moduledoc """
  A codec that speaks Clickhouse's RowBinary format

  To use this codec, set the application `:clickhousex` `:codec` application variable:

       config :clickhousex, codec: Clickhousex.Codec.RowBinary

  """
  alias Clickhousex.{Codec, Codec.Binary.Extractor, Codec.RowBinary.Utils, Codec.Binary}
  import Utils
  use Extractor

  require Record

  @behaviour Codec

  Record.defrecord(:state, column_count: 0, column_names: [], column_types: [], rows: [], count: 0)

  @impl Codec
  def response_format do
    "RowBinaryWithNamesAndTypes"
  end

  @impl Codec
  def request_format do
    "Values"
  end

  @impl Codec
  defdelegate encode(query, params, opts), to: Codec.Values

  @impl Codec
  def decode(state(column_names: column_names, rows: rows, count: count)) do
    {:ok, %{column_names: column_names, rows: Enum.reverse(rows), count: count}}
  end

  def decode(nil) do
    decode(state())
  end

  @impl Codec
  def new do
    nil
  end

  @impl Codec
  def append(nil, data) do
    extract_column_count(data, state())
  end

  def append(state() = state, data) do
    extract_rows(data, state)
  end

  def append({:resume, resumer}, data) do
    case resumer.(data) do
      {:resume, _} = resumer -> resumer
      state() = state -> state
    end
  end

  @extract column_count: :varint
  defp extract_column_count(<<data::binary>>, column_count, state) do
    extract_column_names(data, column_count, state(state, column_count: column_count))
  end

  defp extract_column_names(
         <<data::binary>>,
         0,
         state(column_count: column_count, column_names: column_names) = state
       ) do
    new_state = state(state, column_names: Enum.reverse(column_names))
    extract_column_types(data, column_count, [], new_state)
  end

  defp extract_column_names(<<data::binary>>, remaining, state) do
    extract_column_name(data, remaining, state)
  end

  @extract column_name: :string
  defp extract_column_name(<<data::binary>>, remaining, column_name, state) do
    column_names = state(state, :column_names)

    extract_column_names(
      data,
      remaining - 1,
      state(state, column_names: [column_name | column_names])
    )
  end

  defp extract_column_types(<<data::binary>>, 0, column_types, state) do
    column_types = Enum.reverse(column_types)
    new_state = state(state, column_types: column_types)
    extract_rows(data, new_state)
  end

  defp extract_column_types(<<data::binary>>, remaining, column_types, state) do
    extract_column_type(data, remaining, column_types, state)
  end

  @extract column_type: :string
  defp extract_column_type(<<data::binary>>, remaining, column_type, column_types, state) do
    column_type = parse_type(column_type)

    extract_column_types(data, remaining - 1, [column_type | column_types], state)
  end

  defp extract_rows(<<>>, state() = state) do
    state
  end

  defp extract_rows(<<data::binary>>, state(column_types: column_types) = state) do
    extract_row(data, column_types, [], state)
  end

  defp extract_field(<<>>, type, types, row, state) do
    {:resume, &extract_field(&1, type, types, row, state)}
  end

  defp extract_field(<<data::binary>>, {:fixed_string, length} = fixed_string, types, row, state) do
    case data do
      <<value::binary-size(length), rest::binary>> ->
        extract_row(rest, types, [value | row], state)

      _ ->
        {:resume, &extract_field(data <> &1, fixed_string, types, row, state)}
    end
  end

  # precision defines the number of sub-second digits
  defp extract_field(<<data::binary>>, {:datetime64, precision} = _datetime64, types, row, state)
       when is_integer(precision) do
    {:ok, unix_timestamp, rest} = Binary.decode(data, :i64)

    timestamp =
      cond do
        precision <= 9 ->
          exponent = 9 - precision
          unix_timestamp * trunc(:math.pow(10, exponent))

        true ->
          exponent = precision - 9
          div(unix_timestamp, trunc(:math.pow(10, exponent)))
      end

    elixir_timestamp = timestamp |> DateTime.from_unix!(:nanosecond) |> DateTime.to_naive()
    extract_row(rest, types, [elixir_timestamp | row], state)
  end

  defp extract_field(<<data::binary>>, {:nullable, {:datetime64, precision}} = _datetime64, types, row, state) do
    case Binary.decode(data, :u8) do
      {:ok, 1, rest} ->
        extract_row(rest, types, [nil | row], state)

      {:ok, 0, rest} ->
        extract_field(rest, {:datetime64, precision}, types, row, state)
    end
  end

  defp extract_field(<<0, rest::binary>>, {:nullable, :nothing}, types, row, state) do
    extract_row(rest, types, [nil | row], state)
  end

  defp extract_field(<<1, rest::binary>>, {:nullable, :nothing}, types, row, state) do
    extract_row(rest, types, [nil | row], state)
  end

  @scalar_types [
    :i64,
    :i32,
    :i16,
    :i8,
    :u64,
    :u32,
    :u16,
    :u8,
    :f64,
    :f32,
    :boolean,
    :string,
    :date,
    :datetime
  ]

  @all_types @scalar_types
             |> Enum.flat_map(&type_permutations/1)
             |> Enum.sort()

  # Build all permutations of extract_field/5
  for type <- @all_types do
    defp extract_field(<<data::binary>>, unquote(type), types, row, state) do
      unquote(extractor_name(type))(data, types, row, state)
    end
  end

  # Build all specific typed extractors, e.g. extract_u8/5
  for type <- @all_types do
    @extract field_value: type
    defp unquote(extractor_name(type))(<<data::binary>>, field_value, types, row, state) do
      extract_row(data, types, [field_value | row], state)
    end
  end

  defp extract_row(<<data::binary>>, [], row_data, state(rows: rows, count: count) = state) do
    row = row_data |> Enum.reverse() |> List.to_tuple()
    new_state = state(state, rows: [row | rows], count: count + 1)
    extract_rows(data, new_state)
  end

  defp extract_row(<<data::binary>>, [type | types], row, state) do
    extract_field(data, type, types, row, state)
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
      {length, rest} ->
        rest
        |> String.replace_suffix(")", "")

        {:fixed_string, length}
    end
  end

  defp parse_type(<<"Array(", type::binary>>) do
    rest_type =
      type
      |> String.replace_suffix(")", "")
      |> parse_type()

    {:array, rest_type}
  end

  defp parse_type(<<"DateTime64(", rest::binary>>) do
    case Integer.parse(rest) do
      {length, rest} ->
        rest
        |> String.replace_suffix(")", "")

        {:datetime64, length}
    end
  end

  defp parse_type("Nothing") do
    :nothing
  end

  # Boolean isn't represented below because clickhouse has no concept
  # of booleans.
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
