defmodule Clickhousex do
  @moduledoc """
  Clickhouse driver for Elixir.


  This module handles the connection to Clickhouse, providing support
  for queries, connection backoff, logging, pooling and
  more.
  """

  alias Clickhousex.Query
  alias Clickhousex.Utils

  @typedoc """
  A connection process name, pid or reference.

  A connection reference is used when making multiple requests to the same
  connection, see `transaction/3`.
  """
  @type conn :: DBConnection.conn()

  @type start_option ::
          {:hostname, String.t()}
          | {:port, :inet.port_number()}
          | {:database, String.t()}
          | {:username, String.t()}
          | {:password, String.t()}
          | {:timeout, timeout}
          | {:ssl, boolean()}
          | {:show_sensitive_data_connection_error, boolean}
          | DBConnection.start_option()

  @type query_option ::
          {:datetime_precision, :dt32 | :dt64 | integer()}
          | DBConnection.option()

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
    opts = Utils.default_opts(opts)
    DBConnection.start_link(Clickhousex.Protocol, opts)
  end

  @spec child_spec(Keyword.t()) :: Supervisor.Spec.spec()
  def child_spec(opts) do
    opts = Utils.default_opts(opts)
    DBConnection.child_spec(Clickhousex.Protocol, opts)
  end

  @spec query(DBConnection.conn(), binary(), list, Keyword.t()) ::
          {:ok, iodata(), Clickhousex.Result.t()} | {:error, Clickhousex.Error.t()}
  def query(conn, statement, params \\ [], opts \\ []) do
    DBConnection.prepare_execute(conn, %Query{name: "", statement: statement}, params, opts)
  end

  @spec query!(DBConnection.conn(), binary(), list, Keyword.t()) ::
          {iodata(), Clickhousex.Result.t()}
  def query!(conn, statement, params \\ [], opts \\ []) do
    DBConnection.prepare_execute!(conn, %Query{name: "", statement: statement}, params, opts)
  end
end
