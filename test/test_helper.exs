ExUnit.start()

defmodule ClickhouseCase do
  use ExUnit.CaseTemplate
  alias Clickhousex, as: CH

  def database(ctx) do
    ctx.case
    |> Module.split()
    |> List.last()
    |> Macro.underscore()
  end

  def table(ctx) do
    table =
      ctx.test
      |> Atom.to_string()
      |> String.downcase()
      |> String.replace(" ", "_")

    "#{database(ctx)}.#{table}"
  end

  def schema(ctx, create_statement) do
    create_statement = parameterize(create_statement, ctx)

    CH.query(ctx.client, create_statement, [])
  end

  def select_all(ctx) do
    select(ctx, "SELECT * from {{table}}")
  end

  def select(ctx, select_statement) do
    select(ctx, select_statement, [])
  end

  def select(ctx, select_statement, params) do
    select_statement = parameterize(select_statement, ctx)
    {:ok, _, _} = CH.query(ctx.client, select_statement, params)
  end

  def insert(ctx, insert_statement, values) do
    insert_statement = parameterize(insert_statement, ctx)
    {:ok, _, _} = CH.query(ctx.client, insert_statement, values)
  end

  defp parameterize(query, ctx) do
    query
    |> String.replace(~r/\{\{\s*database\s*\}\}/, database(ctx))
    |> String.replace(~r/\{\{\s*table\s*\}\}/, table(ctx))
    |> String.trim()
  end

  using do
    quote do
      require unquote(__MODULE__)

      import unquote(__MODULE__),
        only: [
          schema: 2,
          select_all: 1,
          select: 2,
          select: 3,
          insert: 3
        ]
    end
  end

  setup_all do
    {:ok, client} = Clickhousex.start_link([])

    {:ok, client: client}
  end

  setup %{client: client} = ctx do
    db_name = database(ctx)

    on_exit(fn ->
      Clickhousex.query!(client, "DROP DATABASE IF EXISTS #{db_name}", [])
    end)

    {:ok, _, _} = Clickhousex.query(client, "CREATE DATABASE #{db_name}", [])

    {:ok, client: client}
  end
end
