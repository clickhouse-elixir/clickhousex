defmodule Clickhousex.HTTPClient do
  @moduledoc false

  alias Clickhousex.Types

  @selected_queries_regex ~r/^(SELECT|SHOW|DESCRIBE|EXISTS)/i
  @req_headers [{"Content-Type", "text/plain"}]

  def send(query, base_address, timeout, username, password, database) when username != nil do
    opts = [hackney: [basic_auth: {username, password}], timeout: timeout, recv_timeout: timeout]
    send_p(query, base_address, database, opts)
  end

  def send(query, base_address, timeout, username, password, database) when username == nil do
    send_p(query, base_address, database, [timeout: timeout, recv_timeout: timeout])
  end

  defp send_p(query, base_address, database, opts) do
    command = parse_command(query)
    query_normalized = query |> normalize_query(command)
    opts_new = opts ++ [params: %{database: database}]

    res = HTTPoison.request(:post, base_address, query_normalized, @req_headers, opts_new)
    case res do
      {:ok, resp} ->
        cond do
          resp.status_code == 200 ->
            case command do
              :selected ->
                case Poison.decode(resp.body) do
                  {:ok, %{"meta" => meta, "data" => data, "rows" => _rows_count}} ->
                    columns = meta |> Enum.map(fn(%{"name" => name, "type" => _type}) -> name end)
                    rows = data |> Enum.map(fn(data_row) ->
                      meta
                      |> Enum.map(fn(%{"name" => column, "type" => column_type}) ->
                        Types.decode(data_row[column], column_type)
                      end)
                      |> List.to_tuple()
                    end)
                    {command, columns, rows}
                  {:error, reason} -> {:error, reason}
                end
              :updated ->
                {:updated, 1}
            end
          true ->
            {:error, resp.body}
        end
      {:error, error} ->
        {:error, error.reason}
    end
  end

  defp parse_command(query) do
    cond do
      query =~ @selected_queries_regex -> :selected
      true -> :updated
    end
  end

  defp normalize_query(query, command) do
    case command do
      :selected -> query_with_format(query)
      _ -> query
    end
  end

  defp query_with_format(query), do: query <> " FORMAT JSON"
end
