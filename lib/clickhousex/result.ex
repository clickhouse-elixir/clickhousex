defmodule Clickhousex.Result do
  @moduledoc """
  Result struct returned from any successful query. Its fields are:

    * `command` - An atom of the query command
    * `columns` - The column names;
    * `rows` - The result set. A list of lists, each inner list corresponding to a
               row, each element in the inner list corresponds to a column;
    * `num_rows` - The number of fetched or affected rows;
  """

  @type t :: %__MODULE__{
          command: atom,
          columns: [String.t()] | nil,
          rows: [[term] | binary] | nil,
          num_rows: integer | :undefined
        }

  defstruct command: nil, columns: nil, rows: nil, num_rows: :undefined
end
