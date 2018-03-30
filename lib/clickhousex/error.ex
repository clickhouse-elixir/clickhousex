defmodule Clickhousex.Error do
  @moduledoc """
  Defines an error returned from the ODBC adapter.
  """

  defexception [:message, :code, constraint_violations: []]

  @type t :: %__MODULE__{
               message: binary(),
               code: atom() | binary(),
               constraint_violations: Keyword.t
             }

  @doc false
  @spec exception(binary()) :: t()
  def exception({odbc_code, native_code, reason} = message) do
    %__MODULE__{
      message: to_string(reason) <> " | ODBC_CODE " <> to_string(odbc_code) <> " | CLICKHOUSE_CODE " <> to_string(native_code),
      code: get_code(message),
      constraint_violations: get_constraint_violations(to_string reason)
    }
  end

  def exception(message) do
    %__MODULE__{
      message: to_string(message),
      code: get_code(to_string message),
      constraint_violations: get_constraint_violations(to_string message)
    }
  end

  defp get_code({odbc_code, native_code, _reason}) do
    cond do
      native_code == 210 ->
        :connection_refused
      odbc_code !== nil ->
        translate(to_string odbc_code)
      true -> :unknown
    end
  end
  defp get_code(message) do
    case Regex.scan(~r/\nCode: (\d+)/i, message) do
      [[_, code]] -> translate(code)
      _ -> :unknown
    end
  end

  defp translate("57"), do: :table_already_exists
  defp translate("60"), do: :base_table_or_view_not_found
  defp translate("81"), do: :database_does_not_exists
  defp translate("82"), do: :database_already_exists
  defp translate("28000"), do: :invalid_authorization
  defp translate("08" <> _), do: :connection_exception
  defp translate(code), do: code

  defp get_constraint_violations(reason) do
    []
  end
end