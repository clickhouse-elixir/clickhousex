defmodule Clickhousex.Error do
  @moduledoc """
  Defines an error returned from the client.
  """

  defexception message: "", code: 0, constraint_violations: []

  @type t :: %__MODULE__{
          message: binary(),
          code: integer(),
          constraint_violations: Keyword.t()
        }

  def exception(message) do
    %__MODULE__{
      message: to_string(message),
      code: get_code(to_string(message)),
      constraint_violations: get_constraint_violations(message)
    }
  end

  defp get_code(message) do
    case Regex.scan(~r/^Code: (\d+)/i, message) do
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

  defp get_constraint_violations(_reason) do
    []
  end
end
