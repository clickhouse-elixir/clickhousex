defmodule Clickhousex do
  @moduledoc """
  Clickhouse driver for Elixir.


  This module handles the connection to Clickhouse, providing support
  for queries, connection backoff, logging, pooling and
  more.
  """

  alias Clickhousex.Query

  @typedoc """
  A connection process name, pid or reference.

  A connection reference is used when making multiple requests to the same
  connection, see `transaction/3`.
  """
  @type conn :: DBConnection.conn()

  @timeout 60_000
  def timeout(), do: @timeout

  ### PUBLIC API ###

  @doc """
    Connect to ClickHouse.
    `opts` expects a keyword list with zero or more of:
      * `:scheme` - Scheme (:http | :https).
          * default value: :http
      * `:hostname` - The server hostname.
          * default value: localhost
      * `:database` - Database name.
          * default value: "default"
      * `:port` - The server port number.
          * default value: 8123
      * `:username` - Username.
          * default value: empty
      * `:password` - User's password.
          * default value: empty
  """

  @spec start_link(Keyword.t()) :: {:ok, pid} | {:error, term}
  def start_link(opts \\ []) do
    opts = Keyword.put(opts, :show_sensitive_data_on_connection_error, true)
    DBConnection.start_link(Clickhousex.Protocol, opts)
  end

  @spec child_spec(Keyword.t()) :: Supervisor.Spec.spec()
  def child_spec(opts) do
    DBConnection.child_spec(Clickhousex.Protocol, opts)
  end

  @spec query(DBConnection.conn(), binary(), list, Keyword.t()) ::
          {:ok, Clickhousex.Result.t()} | {:error, Exception.t()}
  def query(conn, statement, params, opts \\ []) do
    query = %Query{name: "", statement: statement}

    with {:ok, _, result} <- DBConnection.prepare_execute(conn, query, params, opts) do
      {:ok, result}
    end
  end

  @spec query!(DBConnection.conn(), binary(), list, Keyword.t()) :: Clickhousex.Result.t()
  def query!(conn, statement, params, opts \\ []) do
    case query(conn, statement, params, opts) do
      {:ok, result} -> result
      {:error, err} -> raise err
    end
  end

  ## Helpers
  def defaults(opts) do
    Keyword.put_new(opts, :timeout, @timeout)
  end
end
