defmodule Clickhousex.Codec.JSONTest do
  use ExUnit.Case

  alias Clickhousex.Codec.JSON

  describe "decode/1" do
    test "empty response" do
      decoded =
        %{"meta" => [], "data" => [], "rows" => 0}
        |> Jason.encode!()
        |> JSON.decode()

      assert decoded == {:ok, %{column_names: [], rows: [], count: 0}}
    end

    test "2 + 2 == 4" do
      decoded =
        %{"meta" => [%{"name" => "2+2", "type" => "Int8"}], "data" => [[4]], "rows" => 1}
        |> Jason.encode!()
        |> JSON.decode()

      assert decoded == {:ok, %{column_names: ["2+2"], rows: [{4}], count: 1}}
    end

    test "multiple responses" do
      meta = [
        %{"name" => "id", "type" => "UInt64"},
        %{"name" => "date", "type" => "Date"},
        %{"name" => "array_of_timestamps", "type" => "Array(DateTime)"},
        %{"name" => "null", "type" => "Nullable(Int32)"},
        %{"name" => "nested_null", "type" => "Nullable(Array(Nullable(Float32)))"}
      ]

      data = [
        [0, ~D[2020-01-01], [], nil, nil],
        [1, ~D[2020-02-02], [~N[2020-02-02 14:22:33]], 123, [1.0]],
        [2, ~D[2020-03-03], [], nil, [2.0, nil, 3.0]]
      ]

      decoded =
        %{"meta" => meta, "data" => data, "rows" => 3}
        |> Jason.encode!()
        |> JSON.decode()

      column_names = ["id", "date", "array_of_timestamps", "null", "nested_null"]

      rows = [
        {0, ~D[2020-01-01], [], nil, nil},
        {1, ~D[2020-02-02], [~N[2020-02-02 14:22:33]], 123, [1.0]},
        {2, ~D[2020-03-03], [], nil, [2.0, nil, 3.0]}
      ]

      assert decoded == {:ok, %{column_names: column_names, rows: rows, count: 3}}
    end

    test "unknown type" do
      error =
        %{meta: [%{name: "foo", type: "Foo"}], data: [], rows: 0}
        |> Jason.encode!()
        |> JSON.decode()

      assert error == {:error, {:unknown_type, "Foo"}}
    end

    test "nested unknown type" do
      error =
        %{meta: [%{name: "bars", type: "Array(Bar)"}], data: [], rows: 0}
        |> Jason.encode!()
	|> JSON.decode()

      assert error == {:error, {:unknown_type, "Bar)"}}
    end

    test "unmatched paren" do
      error =
        %{meta: [%{name: "xs", type: "Array(Int16"}], data: [], rows: 0}
        |> Jason.encode!()
        |> JSON.decode()

      assert error == {:error, {:unmatched_paren, "Int16"}}
    end

    test "garbage after type" do
      error =
        %{meta: [%{name: "ys", type: "Nullable(Int64)Garbage"}], data: [], rows: 0}
        |> Jason.encode!()
        |> JSON.decode()

      assert error == {:error, {:garbage, "Garbage"}}
    end
  end
end
