defmodule Clickhousex.HTTPClient do
  @moduledoc false

  alias Clickhousex.Types

  @selected_queries_regex ~r/^(SELECT|SHOW|DESCRIBE|EXISTS)/i
  @req_headers [{"Content-Type", "text/plain"}]

  def send(query, base_address, timeout, nil, _password, database) do
    send_p(query, base_address, database, timeout: timeout, recv_timeout: timeout)
  end

  def send(query, base_address, timeout, username, password, database) do
    opts = [hackney: [basic_auth: {username, password}], timeout: timeout, recv_timeout: timeout]
    send_p(query, base_address, database, opts)
  end

  defp send_p(query, base_address, database, opts) do
    command = parse_command(query)
    query_normalized = query |> normalize_query(command)
    opts_new = opts ++ [params: %{database: database}]

    with {:ok, %{status_code: 200, body: body}} <-
           HTTPoison.post(base_address, query_normalized, @req_headers, opts_new),
         {:command, :selected} <- {:command, command},
         {:ok, %{"meta" => meta, "data" => data, "rows" => _rows_count}} <- Jason.decode(body) do
      columns = Enum.map(meta, &{&1["name"], &1["type"]})

      rows =
        for row <- data do
          for {column_name, column_type} <- columns do
            value = Map.get(row, column_name)
            Types.decode(value, column_type)
          end
          |> List.to_tuple()
        end

      {command, Enum.map(meta, & &1["name"]), rows}
    else
      {:command, :updated} ->
        {:updated, 1}

      {:ok, response} ->
        {:error, response.body}

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
