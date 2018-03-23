defmodule Clickhousex.Connection do
  defdelegate start_link(opts), to: Clickhousex
  defdelegate query(conn, statement), to: Clickhousex
  defdelegate query(conn, statement, params), to: Clickhousex
  defdelegate query(conn, statement, params, opts), to: Clickhousex
  defdelegate query!(conn, statement), to: Clickhousex
  defdelegate query!(conn, statement, params), to: Clickhousex
  defdelegate query!(conn, statement, params, opts), to: Clickhousex
end