defmodule Clickhousex.Codec.Values do
  alias Clickhousex.Query

  def encode(%Query{param_count: 0}, []) do
    ""
  end

  def encode(%Query{param_count: 0}, _) do
    raise ArgumentError, "Extra params! Query doesn't contain '?'"
  end

  def encode(%Query{param_count: param_count, substitutions: substitutions}, params) do
    if length(params) != param_count do
      raise ArgumentError,
            "The number of parameters does not correspond to the number of question marks!"
    end

    substitutions
    |> String.split("?")
    |> weave(params)
  end

  defp weave(query_parts, params) do
    weave(query_parts, params, [])
  end

  defp weave([part], [], acc) do
    Enum.reverse([part | acc])
  end

  defp weave([part | parts], [param | params], acc) do
    weave(parts, params, [encode_param(param), part | acc])
  end

  @doc false
  defp encode_param(param) when is_list(param) do
    values = Enum.map_join(param, ",", &encode_param/1)

    "[" <> values <> "]"
  end

  defp encode_param(param) when is_integer(param) do
    Integer.to_string(param)
  end

  defp encode_param(param) when is_boolean(param) do
    to_string(param)
  end

  defp encode_param(param) when is_float(param) do
    to_string(param)
  end

  defp encode_param(param) when is_float(param) do
    to_string(param)
  end

  defp encode_param(nil) do
    "NULL"
  end

  defp encode_param(%DateTime{} = datetime) do
    iso_date =
      datetime
      |> DateTime.truncate(:second)
      |> DateTime.to_iso8601()
      |> String.replace("Z", "")

    "'#{iso_date}'"
  end

  defp encode_param(%NaiveDateTime{} = naive_datetime) do
    naive =
      naive_datetime
      |> NaiveDateTime.truncate(:second)
      |> NaiveDateTime.to_iso8601()

    "'#{naive}'"
  end

  defp encode_param(%Date{} = date) do
    "'#{Date.to_iso8601(date)}'"
  end

  defp encode_param(param) do
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
