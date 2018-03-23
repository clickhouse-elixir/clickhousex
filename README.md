# Clickhousex

ClickHouse database driver to connect with Elixir application by ODBC
interface.

# ClickHouse ODBC driver configuration

Primarily, you need to build and configure the ClickHouse ODBC driver: https://github.com/yandex/clickhouse-odbc. On MacOS typical commands to build the driver are (https://github.com/yandex/clickhouse-odbc/issues/35#issuecomment-338243661):
```
brew install unixodbc
git clone https://github.com/yandex/clickhouse-odbc
cd clickhouse-odbc
git checkout macos-build
git submodule init
git submodule update
mkdir build
cd build
cmake .. -DODBC_LIBRARIES=/usr/local/lib/libodbc.a
make -j8 clickhouse-odbc
```

Typical ODBC config looks like this (MacOS):

```
$ cat /usr/local/etc/odbc.ini
[ODBC Data Sources]
AdTracker = "ClickHouse AdTracker"

[ClickHouse]
Driver = /Users/kgrabar/Projects/AdTracker/clickhouse-odbc/build/driver/clickhouse-odbc.dylib
Description = ClickHouse driver
DATABASE = default
SERVER = 127.0.0.1
PORT = 8123
FRAMED = 0

$ cat /usr/local/etc/odbcinst.ini
[ODBC Drivers]
ClickHouseDriver=Installed

[ClickHouseDriver]
Description=driver_description
Driver=/Users/kgrabar/Projects/AdTracker/clickhouse-odbc/build/driver/clickhouse-odbc.dylib
```

Typical command to launch ClickHouse from docker:
```
docker run --name some-clickhouse-server -p 9000:9000 -p 8123:8123 --ulimit nofile=262144:262144 -v /usr/local/etc/clickhouse-server/config.xml:/etc/clickhouse-server/config.xml yandex/clickhouse-server
```

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

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/clickhousex](https://hexdocs.pm/clickhousex).
