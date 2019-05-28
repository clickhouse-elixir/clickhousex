defmodule Clickhousex.Query do
  @moduledoc """
  Query struct returned from a successfully prepared query.
  """

  @type t :: %__MODULE__{
          name: iodata,
          type: :select | :insert | :alter | :create | :drop,
          query_part: iodata,
          post_body_part: iodata,
          param_count: integer,
          params: iodata | nil,
          columns: [String.t()] | nil
        }

  defstruct name: nil,
            statement: "",
            type: :select,
            params: [],
            param_count: 0,
            post_body_part: "",
            query_part: "",
            columns: []

  def new(statement) do
    %__MODULE__{statement: statement, post_body_part: ""}
    |> DBConnection.Query.parse([])
  end
end

defimpl DBConnection.Query, for: Clickhousex.Query do
  @values_regex ~r/VALUES/i
  @query_type_regex ~r/^(\w*).*/

  @codec Application.get_env(:clickhousex, :codec, Clickhousex.Codec.JSON)

  def parse(%{statement: statement} = query, _opts) do
    param_count =
      statement
      |> String.codepoints()
      |> Enum.count(fn s -> s == "?" end)

    query = %{query | type: query_type(statement)}

    {query_part, post_body_part} = do_parse(query)

    %{query | param_count: param_count, query_part: query_part, post_body_part: post_body_part}
  end

  def describe(query, _opts) do
    query
  end

  def encode(query, params, _opts) do
    @codec.encode(query, params)
  end

  def decode(_query, result, _opts) do
    result
  end

  defp do_parse(%{type: :insert, statement: statement}) do
    with true <- Regex.match?(@values_regex, statement),
         [fragment, substitutions] <- String.split(statement, @values_regex),
         true <- String.contains?(substitutions, "?") do
      {fragment <> " FORMAT #{@codec.request_format}", substitutions}
    else
      _ ->
        {statement, ""}
    end
  end

  defp do_parse(%{type: :select, statement: statement}) do
    {statement, ""}
  end

  defp do_parse(%{type: :alter, statement: statement}) do
    {statement, ""}
  end

  defp do_parse(%{statement: statement}) do
    {statement, ""}
  end

  defp query_type(statement) do
    case Regex.run(@query_type_regex, statement, capture: :all_but_first) do
      [type] ->
        type |> String.downcase() |> String.to_atom()

      _ ->
        :unknown
    end
  end
end

defimpl String.Chars, for: Clickhousex.Query do
  def to_string(%Clickhousex.Query{statement: statement}) do
    IO.iodata_to_binary(statement)
  end
end
