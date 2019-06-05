defmodule Clickhousex.HTTPRequest do
  defstruct post_data: "", query_string_data: ""

  def new() do
    %__MODULE__{}
  end

  def with_post_data(%__MODULE__{} = request, post_data) do
    %{request | post_data: post_data}
  end

  def with_query_string_data(%__MODULE__{} = request, query_string_data) do
    %{request | query_string_data: query_string_data}
  end
end
