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

  def encode(%Query{param_count: param_count, column_count: column_count, type: :insert} = query, params) do
    if length(params) != param_count do
      raise ArgumentError,
            "The number of parameters does not correspond to the number of question marks!"
    end
    
      params
      |> Enum.map(&(encode_param(query, &1)))
      |> Enum.map(&escape_string/1)
      |> Enum.map(&(elem(&1, 1)))
      |> Enum.chunk_every(column_count)
      |> Enum.map(fn line ->
        "(" <> Enum.join(line, ",") <> ")"
      end)
      |> Enum.join(",")
  end

  def encode_parameters(%Query{type: :insert}, _, []) do
    raise ArgumentError, "Function not defined for INSERT statements"
  end


  def encode_parameters(%Query{param_count: 0, statement: statement}, _, []) do
    {statement, nil}
  end

  def encode_parameters(%Query{param_count: 0}, _, _) do
    raise ArgumentError, "Extra params! Query doesn't contain '?'"
  end

  def encode_parameters(%Query{param_count: param_count} = query, query_text, params) do
    if length(params) != param_count do
      raise ArgumentError,
            "The number of parameters does not correspond to the number of question marks!"
    end

    query_parts = String.split(query_text, "?")

    weave(query, query_parts, params)
  end

  defp weave(query, query_parts, params) do
    weave(query, query_parts, params, [], [], 0)
  end

  defp weave(_query, [part], [], query_acc, params_acc, _idx) do
    {Enum.reverse([part | query_acc]), Enum.reverse(params_acc)}
  end

  defp weave(query, [part | parts], [param | params], query_acc, params_acc, idx) do
    {type, param} = encode_param(query, param)
    type_string = "{p" <> to_string(idx) <> ":" <> type <> "}"
    weave(query, parts, params, [type_string, part | query_acc], [{"p" <> to_string(idx), param} | params_acc], idx + 1)
  end

  @doc false
  defp encode_param(query, param) when is_list(param) do
    # Strings have to be always quoted in 
    encoded_params =
      param
      |> Enum.map(&encode_param(query, &1))
      # sting like values have to be always quoted in arrays
      |> Enum.map(&escape_string/1)
    types = Enum.map(encoded_params, &(elem(&1, 0))) |> MapSet.new() |> MapSet.delete("Nullable(UInt8)")

    if MapSet.size(types) != 1 do
      raise ArgumentError, "All elements of an array have to have the same type"
    end

    type = types |> MapSet.to_list() |> hd()

    values = Enum.map_join(encoded_params, ",", &(elem(&1, 1)))

    {"Array(Nullable(#{type}))", "[" <> values <> "]"}
  end

  defp encode_param(_query, param) when is_integer(param) do
    {"Int64", Integer.to_string(param)}
  end

  defp encode_param(_query, true) do
    {"UInt8", "1"}
  end

  defp encode_param(_query, false) do
    {"UInt8", "0"}
  end

  defp encode_param(_query, param) when is_float(param) do
    {"Float", to_string(param)}
  end

  defp encode_param(_query, nil) do
    {"Nullable(UInt8)", "NULL"}
  end

  defp encode_param(_query, %DateTime{} = datetime) do
    # Hier den Typen aus startupconfig nutzen
    iso_date =
      datetime
      |> DateTime.truncate(:second)
      |> DateTime.to_iso8601()
      |> String.replace("Z", "")

    {"DateTime" , iso_date}
  end

  defp encode_param(_query, %NaiveDateTime{} = naive_datetime) do
    naive =
      naive_datetime
      |> NaiveDateTime.truncate(:second)
      |> NaiveDateTime.to_iso8601()

    {"DateTime", naive}
  end

  defp encode_param(_query, %Date{} = date) do
    date_string = Date.to_iso8601(date)
    {"Date", date_string}
  end

  defp encode_param(_query, param) do
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
end
