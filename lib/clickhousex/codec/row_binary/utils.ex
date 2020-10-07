defmodule Clickhousex.Codec.RowBinary.Utils do
  @moduledoc """
  Utility functions for `Clickhousex.Codec.RowBinary`, can not be
  defined in the same module because they are required at compile
  time.
  """

  def type_permutations(type) do
    [
      type,
      {:nullable, type},
      {:array, type},
      {:array, {:nullable, type}}
    ]
  end

  def extractor_name({modifier, base_type}) do
    suffix = type_suffix(base_type)
    :"extract_#{modifier}_#{suffix}"
  end

  def extractor_name(type) when is_atom(type) do
    :"extract_#{type}"
  end

  defp type_suffix({modifier, base_type}) do
    suffix = type_suffix(base_type)
    :"#{modifier}_#{suffix}"
  end

  defp type_suffix(type) when is_atom(type) do
    :"#{type}"
  end
end
