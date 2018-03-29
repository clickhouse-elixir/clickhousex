defmodule Clickhousex.Protocol do
  @moduledoc false

  use DBConnection

  defstruct [pid: nil, conn_opts: []]

  @type state :: %__MODULE__{
                   pid: pid(),
                   conn_opts: Keyword.t
                 }

  @type query :: Clickhousex.Query.t
  @type result :: Clickhousex.Result.t
  @type cursor :: any

  @doc false
  @spec connect(opts :: Keyword.t) :: {:ok, state} |
                                      {:error, Exception.t}
  def connect(opts) do
    driver = opts[:driver] || System.get_env("CLICKHOUSE_ODBC_DRIVER")
    host = opts[:hostname] || "localhost"
    port = opts[:port] || 8123
    database = opts[:database] || "default"
    username = opts[:username] || ""
    password = opts[:password] || ""
    timeout = opts[:timeout] || Clickhousex.timeout()

    conn_str = Enum.reduce([
      {"DRIVER", driver},
      {"SERVER", host},
      {"PORT", port},
      {"USERNAME", username},
      {"PASSWORD", password},
      {"DATABASE", database},
      {"TIMEOUT", timeout}
    ], "", fn {key, value}, acc -> acc <> "#{key}=#{value};" end)

    case Clickhousex.ODBC.start_link(conn_str, opts) do
      {:ok, pid} ->
        {
          :ok,
          %__MODULE__{
            pid: pid,
            conn_opts: opts,
          }
        }
      response -> response
    end
  end

  @doc false
  @spec disconnect(err :: Exception.t, state) :: :ok
  def disconnect(_err, %{pid: pid} = state) do
    case Clickhousex.ODBC.disconnect(pid) do
      :ok -> :ok
      {:error, reason} -> {:error, reason, state}
    end
  end

  @doc false
  @spec ping(state) ::
    {:ok, state} |
    {:disconnect, term, state}
  def ping(state) do
    query = %Clickhousex.Query{name: "ping", statement: "SELECT 1"}
    case do_query(query, [], [], state) do
      {:ok, _, new_state} -> {:ok, new_state}
      {:error, reason, new_state} -> {:disconnect, reason, new_state}
      other -> other
    end
  end

  @doc false
  @spec reconnect(new_opts :: Keyword.t, state) :: {:ok, state}
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
  @spec handle_prepare(query, Keyword.t, state) :: {:ok, query, state}
  def handle_prepare(query, _, state) do
    {:ok, query, state}
  end

  @doc false
  @spec handle_execute(query, list, opts :: Keyword.t, state) ::
          {:ok, result, state} |
          {:error | :disconnect, Exception.t, state}
  def handle_execute(query, params, opts, state) do
    do_query(query, params, opts, state)
  end

  defp do_query(query, params, opts, state) do
    case Clickhousex.ODBC.query(state.pid, query.statement, params, opts) do
      {:error, %Clickhousex.Error{code: :connection_exception} = reason} ->
        {:disconnect, reason, state}
      {:error, reason} ->
        {:error, reason, state}
      {:selected, columns, rows} ->
        {
          :ok,
          %Clickhousex.Result{
            command: :selected,
            columns: Enum.map(columns, &(to_string(&1))),
            rows: rows,
            num_rows: Enum.count(rows)
          },
          state
        }
      {:updated, count} ->
        {
          :ok,
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
          %Clickhousex.Result{
            command: command,
            columns: Enum.map(columns, &(to_string(&1))),
            rows: rows,
            num_rows: Enum.count(rows)
          },
          state
        }
    end
  end

  @doc false
  @spec handle_begin(opts :: Keyword.t, state) :: {:ok, result, state}
  def handle_begin(opts, state) do
    {:ok, %Clickhousex.Result{}, state}
  end

  @doc false
  @spec handle_close(query, Keyword.t, state) :: {:ok, result, state}
  def handle_close(query, opts, state) do
    {:ok, %Clickhousex.Result{}, state}
  end

  @doc false
  @spec handle_commit(opts :: Keyword.t, state) :: {:ok, result, state}
  def handle_commit(opts, state) do
    {:ok, %Clickhousex.Result{}, state}
  end

  @doc false
  @spec handle_info(opts :: Keyword.t, state) :: {:ok, result, state}
  def handle_info(msg, state) do
    {:ok, state}
  end

  @doc false
  @spec handle_rollback(opts :: Keyword.t, state) :: {:ok, result, state}
  def handle_rollback(opts, state) do
    {:ok, %Clickhousex.Result{}, state}
  end

end
