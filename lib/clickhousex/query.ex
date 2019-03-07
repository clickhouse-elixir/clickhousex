defmodule Clickhousex.Query do
  @moduledoc """
  Query struct returned from a successfully prepared query.
  """

  @type t :: %__MODULE__{
          name: iodata,
          statement: iodata,
          columns: [String.t()] | nil
        }

  defstruct [:name, :statement, :columns]
end

defimpl DBConnection.Query, for: Clickhousex.Query do
  # require IEx

  def parse(query, _opts) do
    query
  end

  def describe(query, _opts) do
    query
  end

  #  @spec encode(query :: Clickhousex.Query.t(), params :: [Clickhousex.Type.param()], opts :: Keyword.t()) ::
  #          [Clickhousex.Type.param()]
  def encode(_query, params, _opts) do
    #    Enum.map(params, &(Clickhousex.Type.encode(&1, opts)))
    params
  end

  #  @spec decode(query :: Clickhousex.Query.t(), result :: Clickhousex.Result.t(), opts :: Keyword.t()) ::
  #          Clickhousex.Result.t()
  #  def decode(_query, %Clickhousex.Result{rows: rows} = result, opts) when not is_nil(rows) do
  #    Map.put(result, :rows, Enum.map(rows, fn row -> Enum.map(row, &(Clickhousex.Type.decode(&1, opts))) end))
  #  end
  def decode(_query, result, _opts) do
    result
  end
end

defimpl String.Chars, for: Clickhousex.Query do
  def to_string(%Clickhousex.Query{statement: statement}) do
    IO.iodata_to_binary(statement)
  end
end
