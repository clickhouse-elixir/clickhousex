defmodule Clickhousex.Types do
  @moduledoc false

  def decode(value, type) do
    case type do
      # TODO: implement!
      #"Date" ->
      #  case Date.from_iso8601(to_string(value)) do
      #    {:ok, date} -> {date.year, date.month, date.day}
      #    _ -> value
      #  end
      "DateTime" ->
        case DateTime.from_iso8601(value <> "Z") do
          {:ok, datetime, _} -> {{datetime.year, datetime.month, datetime.day}, {datetime.hour, datetime.minute, datetime.second}}
          _ -> value
        end
      _ -> value
    end
  end
end
