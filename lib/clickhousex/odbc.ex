defmodule Clickhousex.ODBC do

  @moduledoc """
  Adapter to Erlang's `:odbc` module.

  This module is a GenServer that handles communication between Elixir
  and Erlang's `:odbc` module. Transformations are kept to a minimum,
  primarily just translating binaries to charlists and vice versa.

  It is used by `Clickhousex.Protocol` and should not generally be
  accessed directly.
  """

  use GenServer

  alias Clickhousex.Error

  ## Public API

  @doc """
  Starts the connection process to the ODBC driver.

  `conn_str` should be a connection string in the format required by
  your ODBC driver.
  `opts` will be passed verbatim to `:odbc.connect/2`.
  """
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc """
  Sends a parametrized query to the ODBC driver.

  Interface to `:odbc.param_query/3`.See
  [Erlang's ODBC guide](http://erlang.org/doc/apps/odbc/getting_started.html)
  for usage details and examples.

  `pid` is the `:odbc` process id
  `statement` is the SQL query string
  `params` are the parameters to send with the SQL query
  `opts` are options to be passed on to `:odbc`
  """
  @spec query(pid(), iodata(), Keyword.t, Keyword.t) :: {:selected, [binary()], [tuple()]} |
                                                        {:error, Exception.t}
  def query(pid, statement, params, opts) do
    if Process.alive?(pid) do
      GenServer.call(
        pid,
        {:query, %{statement: IO.iodata_to_binary(statement), params: params}},
        Keyword.get(opts, :timeout, 5000)
      )
    else
      {:error, %Clickhousex.Error{message: :no_connection}}
    end
  end

  @doc """
  Commits a transaction on the ODBC driver.

  Note that unless in autocommit mode, all queries are wrapped in
  implicit transactions and must be committed.

  `pid` is the `:odbc` process id
  """
  @spec commit(pid()) :: :ok | {:error, Exception.t}
  def commit(pid) do
    if Process.alive?(pid) do
      GenServer.call(pid, :commit)
    else
      {:error, %Clickhousex.Error{message: :no_connection}}
    end
  end

  @doc """
  Rolls back a transaction on the ODBC driver.

  `pid` is the `:odbc` process id
  """
  @spec rollback(pid()) :: :ok | {:error, Exception.t}
  def rollback(pid) do
    if Process.alive?(pid) do
      GenServer.call(pid, :rollback)
    else
      {:error, %Clickhousex.Error{message: :no_connection}}
    end
  end

  @doc """
  Disconnects from the ODBC driver.

  Attempts to roll back any pending transactions. If a pending
  transaction cannot be rolled back the disconnect still
  happens without any changes being committed.

  `pid` is the `:odbc` process id
  """
  @spec disconnect(pid()) :: :ok
  def disconnect(pid) do
    rollback(pid)
    GenServer.stop(pid, :normal)
  end

  ## GenServer callbacks

  @doc false
  def init(opts) do
#    connect_opts = opts
#                   |> Keyword.delete_first(:conn_str)
#                   |> Keyword.put_new(:auto_commit, :off)
#                   |> Keyword.put_new(:timeout, 5000)
#                   |> Keyword.put_new(:extended_errors, :on)
#                   |> Keyword.put_new(:tuple_row, :off)
#                   |> Keyword.put_new(:binary_strings, :on)
    connect_opts = []
    dsn = "DSN=#{opts[:dsn]};DATABASE=#{opts[:database]};" |> to_charlist
    case handle_errors(:odbc.connect(dsn, connect_opts)) do
      {:ok, pid} -> {:ok, pid}
      {:error, reason} -> {:stop, reason}
    end
  end

  @doc false
  def handle_call({:query, %{statement: statement, params: params}}, _from, state) do
    sql_query = statement |> insert_params_in_query(params) |> to_charlist
    {:reply,
      #handle_errors(:odbc.param_query(state, to_charlist(statement), params)),
      handle_errors(:odbc.sql_query(state, sql_query)),
      state}
  end

  @doc false
  def handle_call(:commit, _from, state) do
    {:reply, handle_errors(:odbc.commit(state, :commit)), state}
  end

  @doc false
  def handle_call(:rollback, _from, state) do
    {:reply, handle_errors(:odbc.commit(state, :rollback)), state}
  end

  @doc false
  def terminate(_reason, state) do
    :odbc.disconnect(state)
  end

  defp handle_errors({:error, reason}), do: {:error, Error.exception(reason)}
  defp handle_errors(term), do: term


  defp insert_params_in_query(query, params) do
    query_parts = String.split(query, "?")
    case length(query_parts) do
      1 ->
        case length(params) do
          0 ->
            query
          _ ->
            raise ArgumentError, "Extra params! Query don't contain '?'"
        end
      len ->
        if (len-1) != length(params) do
          raise ArgumentError, "The number of parameters does not correspond to the number of question marks!"
        end
        param_for_query(query_parts, params)
    end
  end

  defp param_for_query(query_parts, params) when (length(params) == 0) do
    Enum.join(query_parts, "")
  end
  defp param_for_query([query_head|query_tail], [params_head|params_tail]) do
    query_head <> param_as_string(params_head) <> param_for_query(query_tail, params_tail)
  end

  defp param_as_string(param) when is_list(param) do
    param |>
      Enum.map(fn(p) -> param_as_string(p) end) |>
      Enum.join(",")
  end
  defp param_as_string(param) when is_integer(param) do
    Integer.to_string(param)
  end
  defp param_as_string(param) when is_boolean(param) do
    to_string(param)
  end
  defp param_as_string(param) when is_float(param) do
    to_string(param)
  end
  defp param_as_string(param) when is_float(param) do
    to_string(param)
  end
  defp param_as_string({{year, month, day}, {hour, minute, second, _msecond}}) do
    case Ecto.DateTime.cast({{year, month, day}, {hour, minute, second, 0}}) do
      {:ok, date_time} ->
        "'#{Ecto.DateTime.to_string(date_time)}'"
      {:error} ->
        {:error, %Clickhousex.Error{message: :wrong_date_time}}
    end
  end
  defp param_as_string({year, month, day}) do
    #param_as_string({{year, month, day}, {0, 0, 0, 0}})
    case Ecto.Date.cast({year, month, day}) do
      {:ok, date} ->
        "'#{Ecto.Date.to_string(date)}'"
      {:error} ->
        {:error, %Clickhousex.Error{message: :wrong_date}}
    end
  end
  defp param_as_string(param) do
    "'" <> param <> "'"
  end
end
