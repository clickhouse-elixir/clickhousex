defmodule Clickhousex.HTTPClient do
  @moduledoc false

  @get_method_queries_regex ~r/^(SELECT|SHOW|DESCRIBE|EXISTS)/i

  def send(query, base_address, timeout, username, password) when username != nil do
    opts = [hackney: [basic_auth: {username, password}]]
    send_p(query, base_address, timeout, opts)
  end

  def send(query, base_address, timeout, username, password) when username == nil do
    send_p(query, base_address, timeout, [])
  end

  defp send_p(query, base_address, timeout, opts) do
    {func, command} = query |> parse_method_and_command()
    query_escaped = query |> query_with_format() #|> URI.encode()
    opts_new = opts ++ [params: %{query: query_escaped}]
    res = HTTPoison.request(func, base_address, "", [], opts_new)
    case res do
      {:ok, resp} ->
        cond do
          resp.status_code == 200 ->
            case Poison.decode(resp.body) do
              {:ok, %{"meta" => meta, "data" => data, "rows" => _rows_count}} ->
                columns = meta |> Enum.map(fn(%{"name" => name, "type" => _type}) -> name end)
                rows = data |> Enum.map(fn(data_row) ->
                  columns |> Enum.map(fn(column) -> data_row[column] end) |> List.to_tuple()
                end)
                {command, columns, rows}
              {:error, reason} -> {:error, reason}
            end
          true ->
            {:error, resp.body}
        end
      {:error, error} ->
        {:error, error.reason}
    end
  end

  defp parse_method_and_command(query) do
    cond do
      query =~ @get_method_queries_regex -> {:get, :selected}
      true -> {:post, :updated}
    end
  end

  defp query_with_format(query), do: query <> " FORMAT JSON"
end
