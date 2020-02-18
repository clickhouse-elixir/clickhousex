defmodule Clickhousex.Codec do
  @type row :: [term]
  @type query :: Clickhousex.Query.t()
  @type param :: any
  @type param_replacements :: iodata
  @type select_response :: %{
          column_names: [binary],
          rows: [row],
          row_count: non_neg_integer
        }

  @callback response_format() :: String.t()
  @callback request_format() :: String.t()
  @callback decode(any) :: {:ok, select_response} | {:error, any}
  @callback encode(query, param_replacements, [param]) :: iodata
end
