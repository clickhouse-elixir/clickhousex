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
    param
    |> Enum.map(fn p -> param_as_string(p) end)
    |> Enum.join(",")
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

  defp param_as_string({{year, month, day}, {hour, minute, second, _msecond}}) do
    case Ecto.DateTime.cast({{year, month, day}, {hour, minute, second, 0}}) do
      {:ok, date_time} ->
        "'#{Ecto.DateTime.to_string(date_time)}'"

      {:error} ->
        {:error, %Clickhousex.Error{message: :wrong_date_time}}
    end
  end

  defp param_as_string({year, month, day}) do
    # param_as_string({{year, month, day}, {0, 0, 0, 0}})
    case Ecto.Date.cast({year, month, day}) do
      {:ok, date} ->
        "'#{Ecto.Date.to_string(date)}'"

      {:error} ->
        {:error, %Clickhousex.Error{message: :wrong_date}}
    end
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
