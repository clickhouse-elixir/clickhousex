defmodule Clickhousex.Codec.Values do
  alias Clickhousex.Query

  def encode(%Query{param_count: 0} = query, []) do
    %{query | query_part: query.statement}
  end

  def encode(%Query{param_count: 0}, _) do
    raise ArgumentError, "Extra params! Query doesn't contain '?'"
  end

  def encode(
        %Query{type: :insert, post_body_part: post_body, param_count: param_count} = query,
        params
      ) do
    validate_param_count(params, param_count)
    query_parts = String.split(post_body, "?")

    %{query | post_body_part: weave(query, query_parts, params)}
  end

  def encode(
        %Query{param_count: param_count, statement: statement} = query,
        params
      ) do
    validate_param_count(params, param_count)
    query_parts = String.split(statement, "?")

    %{query | query_part: weave(query, query_parts, params), post_body_part: ""}
  end

  defp validate_param_count(params, param_count) do
    if length(params) != param_count do
      raise ArgumentError,
            "The number of parameters does not correspond to the number of question marks!"
    end
  end

  defp weave(query, query_parts, params) do
    weave(query, query_parts, params, [])
  end

  defp weave(_query, [part], [], acc) do
    Enum.reverse([part | acc])
  end

  defp weave(query, [part | parts], [param | params], acc) do
    weave(query, parts, params, [encode_param(query, param), part | acc])
  end

  @doc false
  defp encode_param(query, param) when is_list(param) do
    values = Enum.map_join(param, ",", &encode_param(query, &1))

    case query.type do
      :select ->
        # We pass lists to in clauses, and they shouldn't have brackets around them.
        values

      _ ->
        "[" <> values <> "]"
    end
  end

  defp encode_param(_query, param) when is_integer(param) do
    Integer.to_string(param)
  end

  defp encode_param(_query, true) do
    1
  end

  defp encode_param(_query, false) do
    0
  end

  defp encode_param(_query, param) when is_float(param) do
    to_string(param)
  end

  defp encode_param(_query, param) when is_float(param) do
    to_string(param)
  end

  defp encode_param(_query, nil) do
    "NULL"
  end

  defp encode_param(_query, %DateTime{} = datetime) do
    iso_date =
      datetime
      |> DateTime.truncate(:second)
      |> DateTime.to_iso8601()
      |> String.replace("Z", "")

    "'#{iso_date}'"
  end

  defp encode_param(_query, %NaiveDateTime{} = naive_datetime) do
    naive =
      naive_datetime
      |> NaiveDateTime.truncate(:second)
      |> NaiveDateTime.to_iso8601()

    "'#{naive}'"
  end

  defp encode_param(_query, %Date{} = date) do
    "'#{Date.to_iso8601(date)}'"
  end

  defp encode_param(_query, param) do
    "'" <> escape(param) <> "'"
  end

  defp escape(s) do
    s
    |> String.replace("_", "\_")
    |> String.replace("'", "\'")
    |> String.replace("%", "\%")
    |> String.replace(~s("), ~s(\\"))
    |> String.replace("\\", "\\\\")
  end
end
