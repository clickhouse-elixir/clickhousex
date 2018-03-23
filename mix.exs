defmodule Clickhousex.Mixfile do
  use Mix.Project

  def project do
    [
      app: :clickhousex,
      version: "0.1.0",
      elixir: "~> 1.5",
      deps: deps(),
      name: "Clickhousex",
      description: description(),
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      applications: [:logger, :db_connection, :odbc]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:db_connection, "~> 1.1"}
    ]
  end

  defp description do
    "ClickHouse driver for Elixir."
  end
end
