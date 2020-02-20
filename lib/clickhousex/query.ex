defmodule Clickhousex.Query do
  @moduledoc """
  Query struct returned from a successfully prepared query.
  """

  @type t :: %__MODULE__{
          name: iodata,
          type: :select | :insert | :alter | :create | :drop,
          param_count: integer,
          params: iodata | nil,
          columns: [String.t()] | nil
        }

  defstruct name: nil,
            statement: "",
            type: :select,
            params: [],
            param_count: 0,
            columns: []

  def new(statement) do
    %__MODULE__{statement: statement}
    |> DBConnection.Query.parse([])
  end
end

defimpl DBConnection.Query, for: Clickhousex.Query do
  alias Clickhousex.HTTPRequest

  @values_regex ~r/VALUES/i
  @select_query_regex ~r/\bSELECT\b/i
  @insert_query_regex ~r/\bINSERT\b/i
  @alter_query_regex ~r/\bALTER\b/i

  @codec Application.get_env(:clickhousex, :codec, Clickhousex.Codec.Json)

  def parse(%{statement: statement} = query, _opts) do
    param_count =
      statement
      |> String.codepoints()
      |> Enum.count(fn s -> s == "?" end)

    query = %{query | type: query_type(statement)}

    %{query | param_count: param_count}
  end

  def describe(query, _opts) do
    query
  end

  def encode(%{type: :insert} = query, params, _opts) do
    {query_part, post_body_part} = do_parse(query)
    encoded_params = @codec.encode(query, post_body_part, params)

    HTTPRequest.new()
    |> HTTPRequest.with_query_string_data(query_part)
    |> HTTPRequest.with_post_data(encoded_params)
  end

  def encode(query, params, _opts) do
    {query_part, _post_body_part} = do_parse(query)
    encoded_params = @codec.encode(query, query_part, params)

    HTTPRequest.new()
    |> HTTPRequest.with_query_string_data(encoded_params)
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

  defp do_parse(%{statement: statement}) do
    {statement, ""}
  end

  defp query_type(statement) do
    with {:select, false} <- {:select, Regex.match?(@select_query_regex, statement)},
         {:insert, false} <- {:insert, Regex.match?(@insert_query_regex, statement)},
         {:alter, false} <- {:alter, Regex.match?(@alter_query_regex, statement)} do
      :unknown
    else
      {statement_type, true} ->
        statement_type
    end
  end
end

defimpl String.Chars, for: Clickhousex.Query do
  def to_string(%Clickhousex.Query{statement: statement}) do
    IO.iodata_to_binary(statement)
  end
end
