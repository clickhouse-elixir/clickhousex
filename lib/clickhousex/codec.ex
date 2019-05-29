defmodule Clickhousex.Codec do
  @type row :: tuple
  @type query :: Clickhousex.Query.t()
  @type param :: any
  @type param_replacements :: iodata
  @type select_response :: %{column_names: [String.t()], rows: [row], row_count: non_neg_integer}

  @callback response_format() :: String.t()
  @callback request_format() :: String.t()
  @callback decode(any) :: {:ok, select_response} | {:error, any}
  @callback encode(query, param_replacements, [param]) :: iodata
end
