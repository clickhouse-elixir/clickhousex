defmodule Clickhousex.HTTPClient do
  alias Clickhousex.Query
  @moduledoc false

  @codec Application.get_env(:clickhousex, :codec, Clickhousex.Codec.JSON)

  @req_headers [{"Content-Type", "text/plain"}]

  def send(query, request, base_address, timeout, nil, _password, database) do
    send_p(query, request, base_address, database, timeout: timeout, recv_timeout: timeout)
  end

  def send(query, request, base_address, timeout, username, password, database) do
    opts = [hackney: [basic_auth: {username, password}], timeout: timeout, recv_timeout: timeout]
    send_p(query, request, base_address, database, opts)
  end

  defp send_p(query, request, base_address, database, opts) do
    command = parse_command(query)

    post_body =
      case query.type do
        :select ->
          [request.post_data, " FORMAT ", @codec.response_format]

        _ ->
          [request.post_data]
      end

    http_opts =
      Keyword.put(opts, :params, %{
        database: database,
        query: IO.iodata_to_binary(request.query_string_data)
      })

    with {:ok, %{status_code: 200, body: body}} <-
           HTTPoison.post(base_address, post_body, @req_headers, http_opts),
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

  defp parse_command(%Query{type: :select}) do
    :selected
  end

  defp parse_command(_) do
    :updated
  end
end
