defmodule Clickhousex.HTTPClient do
  @moduledoc false

  @codec Application.get_env(:clickhousex, :codec, Clickhousex.Codec.JSON)

  @selected_queries_regex ~r/^(SELECT|SHOW|DESCRIBE|EXISTS)/i
  @req_headers [{"Content-Type", "text/plain"}]

  def send(query, base_address, timeout, nil, _password, database) do
    send_p(query, base_address, database, timeout: timeout, recv_timeout: timeout)
  end

  def send(query, base_address, timeout, username, password, database) do
    opts = [hackney: [basic_auth: {username, password}], timeout: timeout, recv_timeout: timeout]
    send_p(query, base_address, database, opts)
  end

  defp send_p({query_fragment, params}, base_address, database, opts) do
    command = parse_command(query_fragment)

    params = maybe_append_format(command, params)

    http_opts =
      Keyword.put(opts, :params, %{
        database: database,
        query: query_fragment
      })

    with {:ok, %{status_code: 200, body: body}} <-
           HTTPoison.post(base_address, params, @req_headers, http_opts),
         {:command, :selected} <- {:command, command},
         {:ok, %{column_names: column_names, rows: rows}} <- @codec.decode(body) do
      {command, column_names, rows}
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

  defp maybe_append_format(:selected, query) do
    [query, " FORMAT ", @codec.response_format]
  end

  defp maybe_append_format(_, query) do
    query
  end
end
