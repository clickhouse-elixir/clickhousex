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
          * default value: `/usr/local/lib/libclickhouseodbc.so`
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

  @spec query(conn, iodata, list, Keyword.t) :: {:ok, Clickhousex.Result.t} | {:error, term}
  def query(conn, statement, params, opts \\ []) do
    query = %Clickhousex.Query{ref: make_ref(), name: "", statement: statement}
    execute(conn, query, params, opts)
  end

  @spec query!(conn, iodata, list, Keyword.t) :: Clickhousex.Result.t
  def query!(conn, statement, params, opts \\ []) do
    query = %Clickhousex.Query{ref: make_ref(), name: "", statement: statement}
    execute!(conn, query, params, opts)
  end

  @spec prepare(conn, iodata, iodata, Keyword.t) :: {:ok, Clickhousex.Query.t} | {:error, term}
  def prepare(conn, name, statement, opts \\ []) do
    query = %Clickhousex.Query{name: name, statement: statement}
    opts = opts
           |> defaults()
           |> Keyword.put(:function, :prepare)
    DBConnection.prepare(conn, query, opts)
  end

  @spec prepare!(conn, iodata, iodata, Keyword.t) :: Clickhousex.Query.t
  def prepare!(conn, name, statement, opts \\ []) do
    query = %Clickhousex.Query{name: name, statement: statement}
    opts = opts
           |> defaults()
           |> Keyword.put(:function, :prepare)
    DBConnection.prepare!(conn, query, opts)
  end

  @spec execute(conn, Clickhousex.Query.t, list, Keyword.t) ::
    {:ok, Clickhousex.Result.t} | {:error, term}
  def execute(conn, query, params, opts \\ []) do
    DBConnection.execute(conn, query, params, defaults(opts))
  end

  @spec execute!(conn, Clickhousex.Query.t, list, Keyword.t) :: Clickhousex.Result.t
  def execute!(conn, query, params, opts \\ []) do
    DBConnection.execute!(conn, query, params, defaults(opts))
  end

  @spec close(conn, Clickhousex.Query.t, Keyword.t) :: :ok | {:error, term}
  def close(conn, query, opts \\ []) do
    DBConnection.close(conn, query, defaults(opts))
  end

  @spec close!(conn, Clickhousex.Query.t, Keyword.t) :: :ok
  def close!(conn, query, opts \\ []) do
    DBConnection.close!(conn, query, defaults(opts))
  end

  @spec transaction(conn, ((DBConnection.t) -> result), Keyword.t) ::
    {:ok, result} | {:error, any} when result: var
  def transaction(conn, fun, opts \\ []) do
    DBConnection.transaction(conn, fun, defaults(opts))
  end

  @spec rollback(DBConnection.t, any) :: no_return()
  defdelegate rollback(conn, any), to: DBConnection

  @spec parameters(conn, Keyword.t) :: %{binary => binary}
  def parameters(conn, opts \\ []) do
    DBConnection.execute!(conn, %{}, nil, defaults(opts))
  end

  @spec child_spec(Keyword.t) :: Supervisor.Spec.spec
  def child_spec(opts) do
    DBConnection.child_spec(Clickhousex.Protocol, opts)
  end

#  @spec stream(DBConnection.t, iodata | Clickhousex.Query.t, list, Keyword.t) ::
#    Clickhousex.Stream.t
#  def stream(%DBConnection{} = conn, query, params, options \\ [])  do
#    options =
#      options
#      |> defaults()
#      |> Keyword.put_new(:max_rows, @max_rows)
#    %Clickhousex.Stream{conn: conn, query: query, params: params, options: options}
#  end

  ## Helpers
  defp defaults(opts) do
    Keyword.put_new(opts, :timeout, @timeout)
  end
end
