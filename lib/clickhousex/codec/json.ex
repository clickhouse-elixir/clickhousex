defmodule Clickhousex.Codec.JSON do
  @behaviour Clickhousex.Codec

  @impl true
  def request_format do
    "Values"
  end

  @impl true
  def response_format do
    "JSONCompact"
  end

  @impl true
  def new do
    []
  end

  @impl true
  def append(state, data) do
    [state, data]
  end

  @impl true
  def decode(response) do
    with {:ok, %{"meta" => meta, "data" => data, "rows" => row_count}} <- Jason.decode(response),
         {:ok, column_parsers} <- get_parsers(meta) do
      column_names = Enum.map(IO.inspect(meta, label: "META"), & &1["name"])

      rows =
        for row <- data do
          row
          |> Enum.zip(column_parsers)
          |> Enum.map(fn {raw_value, parser} -> parser.(raw_value) end)
          |> List.to_tuple()
        end

      {:ok, %{column_names: column_names, rows: rows, count: row_count}}
    end
  end

  @impl true
  defdelegate encode(query, replacements, params), to: Clickhousex.Codec.Values

  @spec get_parsers([map], [(term -> term)]) :: {:ok, [(term -> term)]} | {:error, term}
  defp get_parsers(meta, acc \\ [])

  defp get_parsers([], acc), do: {:ok, Enum.reverse(acc)}

  defp get_parsers([%{"type" => type} | meta], acc) do
    case get_parser(type) do
      {:ok, parser, ""} ->
        get_parsers(meta, [parser | acc])

      {:ok, _parser, rest} ->
        {:error, {:garbage, rest}}

      {:error, _} = error ->
        error
    end
  end

  @literal_types ~w(Int8 Int16 Int32 Int64 UInt8 UInt16 UInt32 UInt64 Float32 Float64)

  @spec get_parser(String.t()) :: {:ok, (term -> term), String.t()} | {:error, term}
  for type <- @literal_types do
    defp get_parser(unquote(type) <> rest), do: {:ok, &Function.identity/1, rest}
  end

  defp get_parser("DateTime" <> rest), do: {:ok, &NaiveDateTime.from_iso8601!/1, rest}

  defp get_parser("Date" <> rest), do: {:ok, &Date.from_iso8601!/1, rest}

  defp get_parser("Nullable(" <> rest) do
    case get_parser(rest) do
      {:ok, parser, ")" <> rest} ->
        {:ok, &if(&1, do: parser.(&1)), rest}

      {:ok, _parser, _rest} ->
        {:error, {:unmatched_paren, rest}}

      {:error, _} = error ->
        error
    end
  end

  defp get_parser("Array(" <> rest) do
    case get_parser(rest) do
      {:ok, parser, ")" <> rest} ->
        {:ok, &Enum.map(&1, parser), rest}

      {:ok, _parser, _rest} ->
        {:error, {:unmatched_paren, rest}}

      {:error, _} = error ->
        error
    end
  end

  defp get_parser(type), do: {:error, {:unknown_type, type}}
end
