# Clickhousex

ClickHouse database driver to connect with Elixir application by HTTP interface.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `clickhousex` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:clickhousex, "~> 0.1.0"}
  ]
end
```

## Start driver
Call start_link()/1 function and pass connection options:

```elixir
Clickhousex.start_link([scheme: :http, hostname: "localhost", port: 8132, username: "user", password: "654321"])
```

Options expects a keyword list with zero or more of:

      * `scheme` - Scheme (:http | :https). Default value: :http
      * `hostname` - The server hostname. Default value: "localhost"
      * `port` - The server port number. Default value: 8123
      * `username` - Username. Default value: nil
      * `password` - User's password. Default value: nil


Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/clickhousex](https://hexdocs.pm/clickhousex).
