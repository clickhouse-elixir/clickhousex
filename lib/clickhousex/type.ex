defmodule Clickhousex.Type do
  @moduledoc """
  Type conversions.
  """

  @typedoc "Input param."
  @type param :: bitstring()
                 | number()
                 | date()
                 | time()
                 | datetime()

  @typedoc "Output value."
  @type return_value :: bitstring()
                        | integer()
                        | date()
                        | datetime()

  @typedoc "Date as `{year, month, day}`"
  @type date :: {1..9_999, 1..12, 1..31}

  @typedoc "Time as `{hour, minute, sec, usec}`"
  @type time :: {0..24, 0..60, 0..60, 0..999_999}

  @typedoc "Datetime"
  @type datetime :: {date(), time()}

  @doc """
  Transforms input params into `:odbc` params.
  """
  @spec encode(value :: param(), opts :: Keyword.t) ::
          {:odbc.odbc_data_type(), [:odbc.value()]}
  def encode(value, _) when is_boolean(value) do
    {:sql_bit, [value]}
  end

  def encode({_year, _month, _day} = date, _) do
    encoded = Date.from_erl!(date)
              |> to_string
              |> :unicode.characters_to_binary(:unicode, :latin1)
    {{:sql_varchar, String.length(encoded)}, [encoded]}
  end

  def encode({hour, minute, sec, usec}, _) do
    precision = if usec == 0, do: 0, else: 6
    encoded = Time.from_erl!({hour, minute, sec}, {usec, precision})
              |> to_string
              |> :unicode.characters_to_binary(:unicode, :latin1)
    {{:sql_varchar, String.length(encoded)}, [encoded]}
  end

  def encode({{year, month, day}, {hour, minute, sec, usec}}, _) do
    precision = if usec == 0, do: 0, else: 2
    encoded = NaiveDateTime.from_erl!(
                {{year, month, day}, {hour, minute, sec}}, {usec, precision})
              |> to_string
              |> :unicode.characters_to_binary(:unicode, :latin1)
    {{:sql_varchar, String.length(encoded)}, [encoded]}
  end

  def encode(value, _) when is_integer(value)
                            and (value > -1_000_000_000)
                            and (value < 1_000_000_000) do
    {:sql_integer, [value]}
  end

  def encode(value, _) when is_integer(value) do
    encoded = value |> to_string |> :unicode.characters_to_binary(:unicode, :latin1)
    {{:sql_varchar, String.length(encoded)}, [encoded]}
  end

  def encode(value, _) when is_float(value) do
    encoded = value |> to_string |> :unicode.characters_to_binary(:unicode, :latin1)
    {{:sql_varchar, String.length(encoded)}, [encoded]}
  end

  def encode(value, _) when is_binary(value) do
    with utf16 when is_bitstring(utf16) <-
           :unicode.characters_to_binary(value, :unicode, {:utf16, :little})
      do
      {{:sql_wvarchar, byte_size(value)}, [utf16]}
    else
      _ -> raise %Clickhousex.Error{
        message: "failed to convert string to UTF16LE"}
    end
  end

  def encode(nil, _) do
    {:sql_integer, [:null]}
  end

  def encode(value, _) do
    raise %Clickhousex.Error{
      message: "could not parse param #{inspect value} of unrecognised type."}
  end

  def decode(value, opts) when is_binary(value) do
    if opts[:preserve_encoding] || String.printable?(value) do
      value
    else
      :unicode.characters_to_binary(value, {:utf16, :little}, :unicode)
    end
  end

  def decode(value, _) when is_list(value) do
    to_string(value)
  end

  def decode(:null, _) do
    nil
  end

  def decode({date, {h, m, s}}, _) do
    {date, {h, m, s, 0}}
  end

  def decode(value, _) do
    value
  end
end