defmodule Clickhousex.Protocol do
  @moduledoc false

  use DBConnection

  alias Clickhousex.HTTPClient, as: Client
  alias Clickhousex.Helpers
  alias Clickhousex.Error

  defstruct conn_opts: [], base_address: ""

  @type state :: %__MODULE__{
          conn_opts: Keyword.t(),
          base_address: String.t()
        }

  @type query :: Clickhousex.Query.t()
  @type result :: Clickhousex.Result.t()
  @type cursor :: any

  @doc false
  @spec connect(opts :: Keyword.t()) ::
          {:ok, state}
          | {:error, Exception.t()}
  def connect(opts) do
    scheme = opts[:scheme] || :http
    hostname = opts[:hostname] || "localhost"
    port = opts[:port] || 8123
    database = opts[:database] || "default"
    username = opts[:username] || nil
    password = opts[:password] || nil
    timeout = opts[:timeout] || Clickhousex.timeout()

    base_address = build_base_address(scheme, hostname, port)

    case Client.send("SELECT 1", base_address, timeout, username, password, database) do
      {:selected, _, _} ->
        {
          :ok,
          %__MODULE__{
            conn_opts: [
              scheme: scheme,
              hostname: hostname,
              port: port,
              database: database,
              username: username,
              password: password,
              timeout: timeout
            ],
            base_address: base_address
          }
        }

      resp ->
        resp
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
    query = %Clickhousex.Query{name: "ping", statement: "SELECT 1"}

    case do_query(query, [], [], state) do
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
    do_query(query, params, opts, state)
  end

  defp do_query(query, params, _opts, state) do
    base_address = state.base_address
    username = state.conn_opts[:username]
    password = state.conn_opts[:password]
    timeout = state.conn_opts[:timeout]
    database = state.conn_opts[:database]

    sql_query = query.statement |> IO.iodata_to_binary() |> Helpers.bind_query_params(params)

    res =
      sql_query
      |> Client.send(base_address, timeout, username, password, database)
      |> handle_errors()

    case res do
      {:error, %Error{code: :connection_exception} = reason} ->
        {:disconnect, reason, state}

      {:error, reason} ->
        {:error, reason, state}

      {:selected, columns, rows} ->
        {
          :ok,
          query,
          %Clickhousex.Result{
            command: :selected,
            columns: columns,
            rows: rows,
            num_rows: Enum.count(rows)
          },
          state
        }

      {:updated, count} ->
        {
          :ok,
          query,
          %Clickhousex.Result{
            command: :updated,
            columns: ["count"],
            rows: [[count]],
            num_rows: 1
          },
          state
        }

      {command, columns, rows} ->
        {
          :ok,
          query,
          %Clickhousex.Result{
            command: command,
            columns: columns,
            rows: rows,
            num_rows: Enum.count(rows)
          },
          state
        }
    end
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
  defp handle_errors({:error, reason}), do: {:error, Error.exception(reason)}
  defp handle_errors(term), do: term

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

  ## Private functions

  defp build_base_address(scheme, hostname, port) do
    "#{Atom.to_string(scheme)}://#{hostname}:#{port}/"
  end
end
