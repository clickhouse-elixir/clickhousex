defmodule QueryTest do
  use ExUnit.Case, async: true

  test "simple query" do
    opts = [dsn: "AdTracker"]
    {:ok, pid} = Clickhousex.start_link(opts)
    Clickhousex.Connection.query(pid, "SELECT 1;", [], [])
  end
end