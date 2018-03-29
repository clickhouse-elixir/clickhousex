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
  @type conn :: DBConnection.conn

  @timeout 60_000
  def timeout(), do: @timeout

  ### PUBLIC API ###

  @doc """
    Connect to ClickHouse using ODBC.
    `opts` expects a keyword list with zero or more of:
      * `:driver` - The driver the adapter will use.
          * default value: value of environment variable `CLICKHOUSE_ODBC_DRIVER`
      * `:hostname` - The server hostname.
          * default value: localhost
      * `:port` - The server port number.
          * default value: 8123
      * `:database` - The name of the database.
          * default value: `default`
      * `:username` - Username.
          * default value: empty
      * `:password` - User's password.
          * default value: empty
  """

  @spec start_link(Keyword.t) :: {:ok, pid} | {:error, term}
  def start_link(opts) do
    DBConnection.start_link(Clickhousex.Protocol, opts)
  end

  @spec child_spec(Keyword.t) :: Supervisor.Spec.spec
  def child_spec(opts) do
    DBConnection.child_spec(Clickhousex.Protocol, opts)
  end

  @spec query(pid(), binary(), list, Keyword.t) ::
          {:ok, iodata(), Clickhousex.Result.t}
  def query(conn, statement, params, opts \\ []) do
    DBConnection.prepare_execute(conn, %Query{name: "", statement: statement}, params, opts)
  end

  @spec query!(pid(), binary(), list, Keyword.t) ::
          {iodata(), Clickhousex.Result.t}
  def query!(conn, statement, params, opts \\ []) do
    DBConnection.prepare_execute!(conn, %Query{name: "", statement: statement}, params, opts)
  end

  ## Helpers
  def defaults(opts) do
    Keyword.put_new(opts, :timeout, @timeout)
  end
end
