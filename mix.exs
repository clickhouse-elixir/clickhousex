defmodule Clickhousex.Mixfile do
  use Mix.Project

  def project do
    [
      app: :clickhousex,
      version: "0.1.0",
      elixir: "~> 1.5",
      deps: deps(),
      package: package(),
      source_url: "https://github.com/appodeal/clickhousex"
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
      {:db_connection, "~> 1.1"},
      {:ex_doc, ">= 0.0.0", only: :dev}
    ]
  end

  defp package do
    [
      name: "clickhousex",
      description: description(),
      maintainers: maintainers(),
      licenses: ["Apache 2.0"],
      links: %{"GitHub" => "https://github.com/appodeal/clickhousex"}
    ]
  end

  defp description do
    "ClickHouse driver for Elixir (uses ODBC)."
  end

  defp maintainers do
    ["Ivan Zinoviev", "Roman Chudov", "Konstantin Grabar", "Evgeniy Shurmin", "Alexey Lukyanov"]
  end
end
