defmodule Clickhousex.Utils do
  @moduledoc """
  Helper functions for the whole driver
  """

  @timeout 60_000

  @spec default_opts([Clickhousex.start_option()]) :: [Clickhousex.start_option()]
  def default_opts(opts) do
    opts
    |> Keyword.put_new(:show_sensitive_data_on_connection_error, true)
    |> Keyword.put_new(:timeout, @timeout)
    |> Keyword.put_new(:hostname, "localhost")
    |> Keyword.put_new(:database, "default")
    |> Keyword.put_new(:port, 8123)
    |> Keyword.put_new(:scheme, :http)
  end

  @spec default_query_opts([Clickhousex.query_option()]) :: [Clickhouse.query_option()]
  def default_query_opts(opts) do
    opts
    |> Keyword.put_new(:datetime_precision, :dt32)
  end
end
