defmodule Clickhousex.Mixfile do
  use Mix.Project

  def project do
    [
      app: :clickhousex,
      version: "0.1.0",
      elixir: "~> 1.5",
      deps: deps(),
      name: "ClickhousEx",
      description: description(),
      maintainers: maintainers(),
      licenses: ["Apache 2.0"]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      applications: [:logger, :db_connection, :httpoison]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:db_connection, "~> 1.1"},
      {:httpoison, "~> 1.0"},
      {:poison, ">= 1.0.0"}
    ]
  end

  defp description do
    "ClickHouse driver for Elixir (uses HTTP)."
  end

  defp maintainers do
    ["Ivan Zinoviev", "Roman Chudov", "Konstantin Grabar", "Evgeniy Shurmin", "Alexey Lukyanov"]
  end
end
