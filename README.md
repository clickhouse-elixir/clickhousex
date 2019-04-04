# Clickhousex

ClickHouse database driver to connect with Elixir application by HTTP interface.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `clickhousex` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:clickhousex, "~> 0.3.0"}
  ]
end
```

## Start driver
Call start_link()/1 function and pass connection options:

```elixir
Clickhousex.start_link([scheme: :http, hostname: "localhost", port: 8123, database: "default", username: "user", password: "654321"])
```

Options expects a keyword list with zero or more of:

      * `scheme` - Scheme (:http | :https). Default value: :http
      * `hostname` - The server hostname. Default value: "localhost"
      * `database` - Database name. Default value: "default"
      * `port` - The server port number. Default value: 8123
      * `username` - Username. Default value: nil
      * `password` - User's password. Default value: nil

## Queries examples

```elixir
iex(1)> {:ok, pid} = Clickhousex.start_link([scheme: :http, hostname: "localhost", port: 8123, database: "system"])
{:ok, #PID<0.195.0>}
iex(2)> Clickhousex.query(pid, "SHOW TABLES", [])
{:ok, %Clickhousex.Query{columns: nil, name: "", statement: "SHOW TABLES"},
 %Clickhousex.Result{columns: ["name"], command: :selected, num_rows: 23,
  rows: [["asynchronous_metrics"], ["build_options"], ["clusters"], ["columns"],
   ["databases"], ["dictionaries"], ["events"], ["functions"],
   ["graphite_retentions"], ["merges"], ["metrics"], ["models"], ["numbers"],
   ["numbers_mt"], ["one"], ["parts"], ["parts_columns"], ["processes"],
   ["replicas"], ["replication_queue"], ["settings"], ["tables"],
   ["zookeeper"]]}}
iex(3)>
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/clickhousex](https://hexdocs.pm/clickhousex).
