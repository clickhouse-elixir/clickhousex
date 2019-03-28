defmodule Clickhousex.Query do
  @moduledoc """
  Query struct returned from a successfully prepared query.
  """

  @type t :: %__MODULE__{
          name: iodata,
          statement: iodata,
          query_fragment: iodata,
          param_count: integer,
          params: iodata | nil,
          substitutions: String.t(),
          columns: [String.t()] | nil
        }

  defstruct [:name, :statement, :query_fragment, :columns, :params, :substitutions, :param_count]
end

defimpl DBConnection.Query, for: Clickhousex.Query do
  @values_regex ~r/VALUES/i
  @query_type_regex ~r/^(\w*).*/
  @where_clause_regex ~r/WHERE/i
  @update_clause_regex ~r/UPDATE/i

  @codec Application.get_env(:clickhousex, :codec, Clickhousex.Codec.JSON)

  def parse(%{statement: statement} = query, _opts) do
    param_count =
      statement
      |> String.codepoints()
      |> Enum.count(fn s -> s == "?" end)

    {query_fragment, substitutions} =
      statement
      |> query_type()
      |> do_parse(query)

    %{
      query
      | query_fragment: query_fragment,
        param_count: param_count,
        substitutions: substitutions
    }
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

  defp do_parse(:insert, %{statement: statement}) do
    with true <- Regex.match?(@values_regex, statement),
         [fragment, substitutions] <- String.split(statement, @values_regex),
         true <- String.contains?(substitutions, "?") do
      {fragment <> " FORMAT #{@codec.request_format}", substitutions}
    else
      _ ->
        {statement, ""}
    end
  end

  defp do_parse(:select, %{statement: statement}) do
    with [select, substitutions] <- String.split(statement, @where_clause_regex),
         true <- String.contains?(statement, "?") do
      {select, "WHERE " <> substitutions}
    else
      _ ->
        {statement, ""}
    end
  end

  defp do_parse(:alter, %{statement: statement}) do
    with [update, substitutions] <- String.split(statement, @update_clause_regex),
         true <- String.contains?(statement, "?") do
      {update, "UPDATE " <> substitutions}
    else
      _ ->
        {statement, ""}
    end
  end

  defp do_parse(:create, %{statement: statement}) do
    {statement, ""}
  end

  defp do_parse(_, %{statement: statement}) do
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
