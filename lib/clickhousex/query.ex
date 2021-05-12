defmodule Clickhousex.Query do
  @moduledoc """
  Query struct returned from a successfully prepared query.
  """

  @type t :: %__MODULE__{
          name: iodata,
          type: :select | :insert | :alter | :create | :drop,
          param_count: integer,
          params: iodata | nil,
          column_count: integer | nil,
          columns: [String.t()] | nil
        }

  defstruct name: nil,
            statement: "",
            type: :select,
            params: [],
    param_count: nil,
    column_count: 0,
            columns: []

  def new(statement) do
    %__MODULE__{statement: statement}
    |> DBConnection.Query.parse([])
  end
end

defimpl DBConnection.Query, for: Clickhousex.Query do
  alias Clickhousex.HTTPRequest
  alias Clickhousex.Codec.Values

  @values_regex ~r/VALUES/i
  @values_parameter_regex ~r/^(\((\?,)*\?\),)*(\((\?,)*\?\))$/
  @create_query_regex ~r/\bCREATE\b/i
  @insert_select_query_regex ~r/\bINSERT\b.*\bSELECT\b/is
  @select_query_regex ~r/\bSELECT\b/i
  @insert_query_regex ~r/\bINSERT\b/i
  @alter_query_regex ~r/\bALTER\b/i

  @codec Application.get_env(:clickhousex, :codec, Clickhousex.Codec.JSON)

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
    {query_part, values_part} = do_parse(query)
    query = column_count(query, values_part)
    check_parameter_count(query, params)
    encoded_params = @codec.encode(query, params)

    HTTPRequest.new()
    |> HTTPRequest.with_query_string_data(query_part)
    |> HTTPRequest.with_post_data(encoded_params)
  end

  def encode(%{param_count: param_count} = query, params, _opts) when is_integer(param_count) and param_count > 0 do
    {query_part, _post_body_part} = do_parse(query)
    encoded_params = Values.encode_parameters(query, query_part, params)

    HTTPRequest.new()
    |> HTTPRequest.with_post_data(query_part)
    |> HTTPRequest.with_query_params(encoded_params)
    |> HTTPRequest.with_query_in_body
  end

  def encode(query, _params, _opts) do
    {query_part, _post_body_part} = do_parse(query) |> IO.inspect()

    HTTPRequest.new()
    |> HTTPRequest.with_query_string_data(query_part)
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
    with {:create, false} <- {:create, Regex.match?(@create_query_regex, statement)},
         {:insert_select, false} <- {:insert_select, Regex.match?(@insert_select_query_regex, statement)},
         {:select, false} <- {:select, Regex.match?(@select_query_regex, statement)},
         {:insert, false} <- {:insert, Regex.match?(@insert_query_regex, statement)},
         {:alter, false} <- {:alter, Regex.match?(@alter_query_regex, statement)} do
      :unknown
    else
      {statement_type, true} ->
        statement_type
    end
  end

  defp column_count(query, values_part) do
    if not (values_part |> String.replace(" ", "") |> Regex.match?(@values_parameter_regex)) do
      raise ArgumentError, "Only spaces, questionmarks commas and enclosing parantheses are allowed in the VALUES part"
    end

    row_lengths =
    values_part
    |> String.replace(" ", "")
    |> String.replace("(", "")
    |> String.replace(",", "")
    |> String.split(")", trim: true)
    |> Enum.map(&String.length/1)

    if not row_lengths |> MapSet.new() |> MapSet.size() == 1 do
      raise ArgumentError, "All rows in the VALUES part have to be of the same length"
    end
  
    %{query | column_count: hd(row_lengths)}
  end

  defp check_parameter_count(%{column_count: nil}, _params) do
    nil
  end

  defp check_parameter_count(%{column_count: column_count}, params) do
    if rem(params, column_count) != 0 do
      raise ArgumentError, "All columns in the VALUES part have to be the same length"
    end
  end

end

defimpl String.Chars, for: Clickhousex.Query do
  def to_string(%Clickhousex.Query{statement: statement}) do
    IO.iodata_to_binary(statement)
  end
end
