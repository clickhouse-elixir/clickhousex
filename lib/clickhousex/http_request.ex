defmodule Clickhousex.HTTPRequest do
  @moduledoc false

  defstruct post_data: "", query_string_data: "", query_in_body: false, query_params: nil

  def new do
    %__MODULE__{}
  end

  def with_post_data(%__MODULE__{} = request, post_data) do
    %{request | post_data: post_data}
  end

  def with_query_string_data(%__MODULE__{} = request, query_string_data) do
    %{request | query_string_data: query_string_data}
  end

  def with_query_in_body(%__MODULE__{} = request) do
    %{request | query_in_body: true}
  end

  def with_query_params(%__MODULE__{} = request, query_params) do
    %{request | query_params: query_params}
  end
end
