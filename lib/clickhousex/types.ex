defmodule Clickhousex.Types do
  @moduledoc false

  def decode(value, type) do
    case type do
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
