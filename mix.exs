defmodule Clickhousex.Mixfile do
  use Mix.Project

  def project do
    [
      app: :clickhousex,
      version: "0.5.0",
      elixir: "~> 1.5",
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      package: package(),
      source_url: "https://github.com/clickhouse-elixir/clickhousex"
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:db_connection, "~> 2.0"},
      {:mint, "~> 1.0"},
      {:castore, "~> 0.1"},
      {:jason, "~> 1.0"},
      {:ex_doc, "~> 0.22", only: :dev},
      {:benchee, "~> 1.0", only: [:dev, :test]},
      {:credo, "~> 1.5", only: :dev}
      # {:nicene, "~> 0.4.0", only: :dev}
    ]
  end

  defp package do
    [
      name: "clickhousex",
      description: description(),
      maintainers: maintainers(),
      licenses: ["Apache 2.0"],
      links: %{"GitHub" => "https://github.com/clickhouse-elixir/clickhousex"}
    ]
  end

  defp description do
    "ClickHouse driver for Elixir (uses HTTP)."
  end

  defp maintainers do
    [
      "Roman Chudov",
      "Konstantin Grabar",
      "Ivan Zinoviev",
      "Evgeniy Shurmin",
      "Alexey Lukyanov",
      "Yaroslav Rogov",
      "Ivan Sokolov",
      "Georgy Sychev"
    ]
  end
end
