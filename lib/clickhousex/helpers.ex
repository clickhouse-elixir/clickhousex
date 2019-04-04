defmodule Clickhousex.Helpers do
  @moduledoc false

  @doc false
  def bind_query_params(query, params) do
    query_parts = String.split(query, "?")

    case length(query_parts) do
      1 ->
        case params do
          [] ->
            query

          _ ->
            raise ArgumentError, "Extra params! Query don't contain '?'"
        end

      len ->
        if len - 1 != length(params) do
          raise ArgumentError,
                "The number of parameters does not correspond to the number of question marks!"
        end

        param_for_query(query_parts, params)
    end
  end

  @doc false
  defp param_for_query(query_parts, params) when length(params) == 0 do
    Enum.join(query_parts, "")
  end

  defp param_for_query([query_head | query_tail], [params_head | params_tail]) do
    query_head <> param_as_string(params_head) <> param_for_query(query_tail, params_tail)
  end

  @doc false
  defp param_as_string(param) when is_list(param) do
    values = Enum.map_join(param, ",", &param_as_string/1)

    "[" <> values <> "]"
  end

  defp param_as_string(param) when is_integer(param) do
    Integer.to_string(param)
  end

  defp param_as_string(param) when is_boolean(param) do
    to_string(param)
  end

  defp param_as_string(param) when is_float(param) do
    to_string(param)
  end

  defp param_as_string(param) when is_float(param) do
    to_string(param)
  end

  defp param_as_string(%DateTime{} = datetime) do
    DateTime.to_iso8601(datetime)
  end

  defp param_as_string(%Date{} = date) do
    Date.to_iso8601(date)
  end

  defp param_as_string(nil) do
    "NULL"
  end

  defp param_as_string(param) do
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
