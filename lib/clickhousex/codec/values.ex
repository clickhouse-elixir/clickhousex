defmodule Clickhousex.Codec.Values do
  @moduledoc """
  Routines for [Values][1] input/output format.

  **NB**: This module does not implement `Clickhousex.Codec` behaviour.

  [1]: https://clickhouse.tech/docs/en/interfaces/formats/#data-format-values
  """

  alias Clickhousex.Query

  @string_datatypes ~w[Date DateTime String]

  def encode(%Query{param_count: 0, type: :insert}, []) do
    # An insert query's arguments go into the post body and the query part goes into the query string.
    # If we don't have any arguments, we don't have to encode anything, but we don't want to return
    # anything here because we'll duplicate the query into both the query string and post body
    ""
  end

  def encode(%Query{param_count: param_count, column_count: column_count, type: :insert} = query, params, opts) do
    if length(params) != param_count do
      raise ArgumentError,
            "The number of parameters does not correspond to the number of question marks!"
    end

    params
    |> Enum.map(&encode_param(query, &1, opts))
    |> Enum.map(&escape_string/1)
    |> Enum.map(&elem(&1, 1))
    |> Enum.chunk_every(column_count)
    |> Enum.map(fn line ->
      "(" <> Enum.join(line, ",") <> ")"
    end)
    |> Enum.join(",")
  end

  def encode_parameters(%Query{type: :insert}, _, [], _opts) do
    raise ArgumentError, "Function not defined for INSERT statements"
  end

  def encode_parameters(%Query{param_count: 0, statement: statement}, _, [], _opts) do
    {statement, nil}
  end

  def encode_parameters(%Query{param_count: 0}, _, _, _opts) do
    raise ArgumentError, "Extra params! Query doesn't contain '?'"
  end

  def encode_parameters(%Query{param_count: param_count} = query, query_text, params, opts) do
    if length(params) != param_count do
      raise ArgumentError,
            "The number of parameters does not correspond to the number of question marks!"
    end

    query_parts = String.split(query_text, "?")

    weave(query, query_parts, params, opts)
  end

  defp weave(query, query_parts, params, opts) do
    weave(query, query_parts, params, [], [], 0, opts)
  end

  defp weave(_query, [part], [], query_acc, params_acc, _idx, _opts) do
    {Enum.reverse([part | query_acc]), Enum.reverse(params_acc)}
  end

  defp weave(query, [part | parts], [param | params], query_acc, params_acc, idx, opts) do
    {type, param} = encode_param(query, param, opts)
    type_string = "{p" <> to_string(idx) <> ":" <> type <> "}"

    weave(
      query,
      parts,
      params,
      [type_string, part | query_acc],
      [{"p" <> to_string(idx), param} | params_acc],
      idx + 1,
      opts
    )
  end

  @doc false
  defp encode_param(query, param, opts) when is_list(param) do
    # Strings have to be always quoted in 
    encoded_params =
      param
      |> Enum.map(&encode_param(query, &1, opts))
      # sting like values have to be always quoted in arrays
      |> Enum.map(&escape_string/1)

    types = Enum.map(encoded_params, &elem(&1, 0)) |> MapSet.new() |> MapSet.delete("Nullable(UInt8)")

    if MapSet.size(types) != 1 do
      raise ArgumentError, "All elements of an array have to have the same type"
    end

    type = types |> MapSet.to_list() |> hd()

    values = Enum.map_join(encoded_params, ",", &elem(&1, 1))

    {"Array(Nullable(#{type}))", "[" <> values <> "]"}
  end

  # some function parameters need UInt8 and are not happy with Int64
  defp encode_param(_query, param, _opts) when is_integer(param) and param >= 0 and param <= 255 do
    {"UInt8", Integer.to_string(param)}
  end

  defp encode_param(_query, param, _opts) when is_integer(param) do
    {"Int64", Integer.to_string(param)}
  end

  defp encode_param(_query, true, _opts) do
    {"UInt8", "1"}
  end

  defp encode_param(_query, false, _opts) do
    {"UInt8", "0"}
  end

  defp encode_param(_query, param, _opts) when is_float(param) do
    {"Float", to_string(param)}
  end

  defp encode_param(_query, nil, _opts) do
    {"Nullable(UInt8)", "NULL"}
  end

  defp encode_param(_query, %DateTime{} = datetime, opts) do
    datetime_precision = Keyword.get(opts, :datetime_precision)
    dt_type = datetime_type(datetime_precision)

    datetime =
      case datetime_precision do
        :dt32 -> DateTime.truncate(datetime, :second)
        _ -> datetime
      end

    iso_date =
      datetime
      |> DateTime.to_iso8601()
      |> String.replace("Z", "")

    {dt_type, iso_date}
  end

  defp encode_param(_query, %NaiveDateTime{} = naive_datetime, opts) do
    datetime_precision = Keyword.get(opts, :datetime_precision)
    dt_type = datetime_type(datetime_precision)

    datetime =
      case datetime_precision do
        :dt32 -> NaiveDateTime.truncate(naive_datetime, :second)
        _ -> naive_datetime
      end

    naive =
      datetime
      |> NaiveDateTime.to_iso8601()

    {dt_type, naive}
  end

  defp encode_param(_query, %Date{} = date, _opts) do
    date_string = Date.to_iso8601(date)
    {"Date", date_string}
  end

  defp encode_param(_query, param, _opts) do
    {"String", param}
  end

  # When the Values are in the body, the string like values
  # have to be put in quotes
  defp escape_string({type, value}) when type in @string_datatypes do
    {type, "'" <> value <> "'"}
  end

  defp escape_string({type, value}) do
    {type, value}
  end

  defp datetime_type(datetime_precision) do
    case datetime_precision do
      :dt32 ->
        "DateTime"

      :dt64 ->
        "DateTime64"

      precision when is_integer(precision) and precision >= 0 and precision <= 9 ->
        "DateTime64(#{precision})"

      _ ->
        raise ArgumentError, "wrong precision for DateTime"
    end
  end
end
