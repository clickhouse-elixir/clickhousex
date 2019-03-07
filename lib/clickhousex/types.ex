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
    case type do
      "Int64" ->
        String.to_integer(value)

      "Date" ->
        case Date.from_iso8601(value) do
          {:ok, date} -> date
          _ -> {:error, :not_an_iso8601_date}
        end

      "DateTime" ->
        case DateTime.from_iso8601(value <> "Z") do
          {:ok, datetime, _} -> datetime
          _ -> value
        end

      _ ->
        value
    end
  end
end
