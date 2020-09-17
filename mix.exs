defmodule Clickhousex.Mixfile do
  use Mix.Project

  def project do
    [
      app: :clickhousex,
      version: "0.4.0",
      elixir: "~> 1.5",
      deps: deps(),
      package: package(),
      source_url: "https://github.com/appodeal/clickhousex"
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      applications: [:logger, :db_connection]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:db_connection, "~> 2.2"},
      {:mint, "~> 1.1"},
      {:castore, "~> 0.1"},
      {:jason, "~> 1.2"},
      {:ex_doc, "~> 0.22", only: :dev},
      {:benchee, "~> 1.0", only: [:dev, :test]},
      {:credo, "~> 1.2", only: :dev},
      {:nicene, "~> 0.4.0", only: :dev}
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
    "ClickHouse driver for Elixir (uses HTTP)."
  end

  defp maintainers do
    ["Roman Chudov", "Konstantin Grabar", "Ivan Zinoviev", "Evgeniy Shurmin", "Alexey Lukyanov"]
  end
end
