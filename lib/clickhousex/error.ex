defmodule Clickhousex.Error do
  @moduledoc """
  Defines an error returned from the ODBC adapter.
  """

  defexception [:message, :odbc_code, constraint_violations: []]

  @type t :: %__MODULE__{
               message: binary(),
               odbc_code: atom() | binary(),
               constraint_violations: Keyword.t
             }

  @doc false
  @spec exception(binary()) :: t()
  def exception({odbc_code, native_code, reason} = message) do
    %__MODULE__{
      message: to_string(reason) <> " | ODBC_CODE " <> to_string(odbc_code) <> " | CLICKHOUSE_CODE " <> to_string(native_code),
      odbc_code: get_code(message),
      constraint_violations: get_constraint_violations(to_string reason)
    }
  end

  def exception(message) do
    %__MODULE__{
      message: to_string(message)
    }
  end

  defp get_code({odbc_code, native_code, _reason}) do
    cond do
      native_code == 57 ->
        :database_already_exists
      native_code == 60 ->
        :database_does_not_exists
      native_code == 210 ->
        :connection_refused
      odbc_code !== nil ->
        translate(to_string odbc_code)
      true -> :unknown
    end
  end
  defp get_code(_), do: :unknown

#  defp translate("42S01"), do: :base_table_or_view_already_exists
#  defp translate("42S02"), do: :base_table_or_view_not_found
  defp translate("28000"), do: :invalid_authorization
  defp translate("08" <> _), do: :connection_exception
  defp translate(code), do: code

  defp get_constraint_violations(reason) do
    []
  end
end