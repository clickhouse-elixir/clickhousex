defmodule Clickhousex.HTTPClient do
  defmodule Response do
    defstruct ref: nil, codec_state: nil, status: nil, error_buffer: [], complete?: false

    @codec Application.get_env(:clickhousex, :codec, Clickhousex.Codec.JSON)
    def new(ref) do
      codec_state = @codec.new()

      %__MODULE__{ref: ref, codec_state: codec_state}
    end

    def append_messages(%__MODULE__{} = response, messages) do
      Enum.reduce(messages, response, &append(&2, &1))
    end

    def decode(%__MODULE__{status: status, error_buffer: error_buffer}) when status != 200 do
      {:error, IO.iodata_to_binary(error_buffer)}
    end

    def decode(%__MODULE__{codec_state: state, complete?: true}) do
      @codec.decode(state)
    end

    def format do
      @codec.response_format
    end

    defp append(
           %__MODULE__{status: status, ref: ref, error_buffer: error_buffer} = response,
           {:data, ref, data}
         )
         when status != 200 do
      %{response | error_buffer: [error_buffer, data]}
    end

    defp append(%__MODULE__{codec_state: state, ref: ref} = response, {:data, ref, data}) do
      %{response | codec_state: @codec.append(state, data)}
    end

    defp append(%__MODULE__{ref: ref} = response, {:status, ref, status_code}) do
      %{response | status: status_code}
    end

    defp append(%__MODULE__{ref: ref} = response, {:headers, ref, _headers}) do
      response
    end

    defp append(%__MODULE__{ref: ref} = response, {:done, ref}) do
      %{response | complete?: true}
    end
  end

  alias Clickhousex.Query
  @moduledoc false

  @req_headers [{"Content-Type", "text/plain"}]

  def connect(scheme, host, port) do
    Mint.HTTP.connect(scheme, host, port, mode: :passive)
  end

  def disconnect(conn) do
    Mint.HTTP.close(conn)
  end

  def request(conn, query, request, timeout, nil, _password, database) do
    post(conn, query, request, database, timeout: timeout, recv_timeout: timeout)
  end

  def request(conn, query, request, timeout, username, password, database) do
    opts = [basic_auth: {username, password}, timeout: timeout, recv_timeout: timeout]
    post(conn, query, request, database, opts)
  end

  defp post(conn, query, request, database, opts) do
    {recv_timeout, opts} = Keyword.pop(opts, :recv_timeout, 5000)

    query_string =
      URI.encode_query(%{
        database: database,
        query: IO.iodata_to_binary(request.query_string_data)
      })

    path = "/?#{query_string}"
    post_body = maybe_append_format(query, request)
    headers = headers(opts, post_body)

    with {:ok, conn, ref} <- Mint.HTTP.request(conn, "POST", path, headers, post_body),
         {:ok, conn, %Response{} = response} <-
           receive_response(conn, recv_timeout, Response.new(ref)) do
      decode_response(conn, query, response)
    else
      {:error, conn, error, _messages} ->
        {:error, conn, error}
    end
  end

  defp decode_response(conn, %Query{type: :select}, %Response{} = response) do
    case Response.decode(response) do
      {:ok, %{column_names: columns, rows: rows}} -> {:ok, conn, {:selected, columns, rows}}
      {:error, error} -> {:error, conn, error.reason}
    end
  end

  defp decode_response(conn, %Query{}, response) do
    case Response.decode(response) do
      {:error, reason} -> {:error, conn, reason}
      _ -> {:ok, conn, {:updated, 1}}
    end
  end

  defp headers(opts, post_iodata) do
    headers =
      case Keyword.get(opts, :basic_auth) do
        {username, password} ->
          auth_hash = Base.encode64("#{username}:#{password}")
          auth_header = {"Authorization", "Basic: #{auth_hash}"}
          [auth_header | @req_headers]

        nil ->
          @req_headers
      end

    content_length = post_iodata |> IO.iodata_length() |> Integer.to_string()
    [{"content-length", content_length} | headers]
  end

  defp receive_response(conn, _recv_timeout, %Response{complete?: true} = response) do
    {:ok, conn, response}
  end

  defp receive_response(conn, recv_timeout, response) do
    case Mint.HTTP.recv(conn, 0, recv_timeout) do
      {:ok, conn, messages} ->
        response = Response.append_messages(response, messages)
        receive_response(conn, recv_timeout, response)

      {:error, conn, err, messages} ->
        response = Response.append_messages(response, messages)
        {:error, conn, err, response}
    end
  end

  defp maybe_append_format(%Query{type: :select}, request) do
    [request.post_data, " FORMAT ", Response.format()]
  end

  defp maybe_append_format(_, request) do
    [request.post_data]
  end
end
