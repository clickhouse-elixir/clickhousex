defmodule Clickhousex.Protocol do
  @moduledoc false

  use DBConnection

  alias Clickhousex.Error
  alias Clickhousex.HTTPClient, as: Client

  defstruct conn_opts: [], base_address: "", conn: nil

  @type state :: %__MODULE__{
          conn_opts: Keyword.t(),
          base_address: String.t(),
          conn: Mint.HTTP.t()
        }

  @type query :: Clickhousex.Query.t()
  @type result :: Clickhousex.Result.t()
  @type cursor :: any

  @ping_query Clickhousex.Query.new("SELECT 1") |> DBConnection.Query.parse([])
  @ping_params DBConnection.Query.encode(@ping_query, [], [])

  @doc false
  @spec connect(opts :: Keyword.t()) :: {:ok, state} | {:error, Exception.t()}
  def connect(opts) do
    scheme = opts[:scheme]
    hostname = opts[:hostname]
    port = opts[:port]
    database = opts[:database]
    username = opts[:username]
    password = opts[:password]
    timeout = opts[:timeout]

    {:ok, conn} = Client.connect(scheme, hostname, port)

    response = Client.request(conn, @ping_query, @ping_params, timeout, username, password, database)

    with {:ok, conn, {:selected, _, _}} <- response do
      conn_opts = [
        scheme: scheme,
        hostname: hostname,
        port: port,
        database: database,
        username: username,
        password: password,
        timeout: timeout
      ]

      state = %__MODULE__{
        conn: conn,
        conn_opts: conn_opts
      }

      {:ok, state}
    end
  end

  @doc false
  @spec disconnect(err :: Exception.t(), state) :: :ok
  def disconnect(_err, _state) do
    :ok
  end

  @doc false
  @spec ping(state) ::
          {:ok, state}
          | {:disconnect, term, state}
  def ping(state) do
    case do_query(state.conn, @ping_query, @ping_params, [], state) do
      {:ok, _, _, new_state} -> {:ok, new_state}
      {:error, reason, new_state} -> {:disconnect, reason, new_state}
      other -> other
    end
  end

  @doc false
  @spec reconnect(new_opts :: Keyword.t(), state) :: {:ok, state}
  def reconnect(new_opts, state) do
    with :ok <- disconnect("Reconnecting", state),
         do: connect(new_opts)
  end

  @doc false
  @spec checkin(state) :: {:ok, state}
  def checkin(state) do
    {:ok, state}
  end

  @doc false
  @spec checkout(state) :: {:ok, state}
  def checkout(state) do
    {:ok, state}
  end

  @doc false
  def handle_status(_, state) do
    {:idle, state}
  end

  @doc false
  @spec handle_prepare(query, Keyword.t(), state) :: {:ok, query, state}
  def handle_prepare(query, _, state) do
    {:ok, query, state}
  end

  @doc false
  @spec handle_execute(query, list, opts :: Keyword.t(), state) ::
          {:ok, result, state}
          | {:error | :disconnect, Exception.t(), state}
  def handle_execute(query, params, opts, state) do
    do_query(state.conn, query, params, opts, state)
  end

  @doc false
  def handle_declare(_query, _params, _opts, state) do
    {:error, :cursors_not_supported, state}
  end

  @doc false
  def handle_deallocate(_query, _cursor, _opts, state) do
    {:error, :cursors_not_supported, state}
  end

  def handle_fetch(_query, _cursor, _opts, state) do
    {:error, :cursors_not_supported, state}
  end

  @doc false
  @spec handle_begin(opts :: Keyword.t(), state) :: {:ok, result, state}
  def handle_begin(_opts, state) do
    {:ok, %Clickhousex.Result{}, state}
  end

  @doc false
  @spec handle_close(query, Keyword.t(), state) :: {:ok, result, state}
  def handle_close(_query, _opts, state) do
    {:ok, %Clickhousex.Result{}, state}
  end

  @doc false
  @spec handle_commit(opts :: Keyword.t(), state) :: {:ok, result, state}
  def handle_commit(_opts, state) do
    {:ok, %Clickhousex.Result{}, state}
  end

  @doc false
  @spec handle_info(opts :: Keyword.t(), state) :: {:ok, result, state}
  def handle_info(_msg, state) do
    {:ok, state}
  end

  @doc false
  @spec handle_rollback(opts :: Keyword.t(), state) :: {:ok, result, state}
  def handle_rollback(_opts, state) do
    {:ok, %Clickhousex.Result{}, state}
  end

  defp do_query(conn, query, params, _opts, state) do
    username = state.conn_opts[:username]
    password = state.conn_opts[:password]
    timeout = state.conn_opts[:timeout]
    database = state.conn_opts[:database]

    res =
      conn
      |> Client.request(query, params, timeout, username, password, database)
      |> handle_errors()

    case res do
      {:error, conn, %Error{code: :connection_exception} = reason} ->
        {:disconnect, reason, %{state | conn: conn}}

      {:error, conn, reason} ->
        {:error, reason, %{state | conn: conn}}

      {:ok, conn, {:selected, columns, rows}} ->
        {
          :ok,
          query,
          %Clickhousex.Result{
            command: :selected,
            columns: columns,
            rows: rows,
            num_rows: Enum.count(rows)
          },
          %{state | conn: conn}
        }

      {:ok, conn, {:updated, count}} ->
        {
          :ok,
          query,
          %Clickhousex.Result{
            command: :updated,
            columns: ["count"],
            rows: [[count]],
            num_rows: 1
          },
          %{state | conn: conn}
        }

      {:ok, conn, {command, columns, rows}} ->
        {
          :ok,
          query,
          %Clickhousex.Result{
            command: command,
            columns: columns,
            rows: rows,
            num_rows: Enum.count(rows)
          },
          %{state | conn: conn}
        }
    end
  end

  @doc false
  defp handle_errors({:error, conn, reason}) do
    {:error, conn, Error.exception(reason)}
  end

  defp handle_errors(term), do: term
end
