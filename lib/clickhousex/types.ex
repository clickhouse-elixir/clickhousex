defmodule Clickhousex.Types do
  @moduledoc false

  def decode(nil, _) do
    nil
  end

  def decode(value, <<"Nullable(", type::binary>>) do
    type = String.replace_suffix(type, ")", "")
    decode(value, type)
  end

  def decode(value, type) do
    case {type, value} do
      {"Float64", val} when is_integer(val) ->
        1.0 * val

      {"Int64", val} ->
        String.to_integer(val)

      {"Date", val} ->
        case Date.from_iso8601(val) do
          {:ok, date} -> date
          _ -> {:error, :not_an_iso8601_date}
        end

      {"DateTime", val} ->
        case DateTime.from_iso8601(val <> "Z") do
          {:ok, datetime, _} -> datetime
          _ -> value
        end

      {_, val} ->
        val
    end
  end
end
