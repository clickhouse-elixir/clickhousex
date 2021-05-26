defmodule Clickhousex.Codec.ExtractorTest do
  use ExUnit.Case
  alias Clickhousex.Codec.Binary

  defmodule Extractors do
    alias Clickhousex.Codec.Binary.Extractor
    use Extractor
    @scalar_types ~w(i64 i32 i16 i8 u64 u32 u16 u8 f64 f32 string boolean)a

    @extract value: :varint
    def extract(<<rest::binary>>, :varint, value) do
      {:ok, value, rest}
    end

    for type <- @scalar_types do
      @extract value: type
      def extract(<<rest::binary>>, unquote(type), value) do
        {:ok, value, rest}
      end
    end

    for base_type <- @scalar_types,
        type = {:nullable, base_type} do
      @extract value: type
      def extract(<<rest::binary>>, unquote(type), value) do
        {:ok, value, rest}
      end
    end

    for base_type <- @scalar_types,
        type = {:list, base_type} do
      @extract value: type
      def extract(<<rest::binary>>, unquote(type), value) do
        {:ok, value, rest}
      end
    end

    for base_type <- @scalar_types,
        type = {:list, {:nullable, base_type}} do
      @extract value: type
      def extract(<<rest::binary>>, unquote(type), value) do
        {:ok, value, rest}
      end
    end
  end

  def encode(type, to_encode) do
    type |> Binary.encode(to_encode) |> IO.iodata_to_binary()
  end

  describe "resuming" do
    test "it should resume varints" do
      first = <<1::size(1), 7::size(7)>>
      rest = <<0::size(1), 14::size(7)>>

      assert {:resume, resume_fn} = Extractors.extract(first, :varint)
      assert {:ok, 1799, <<>>} == resume_fn.(rest)
    end

    test "it should resume ints" do
      assert {:resume, resume_fn} = Extractors.extract(<<56, 0, 0, 0, 0, 0, 0>>, :i64)
      assert {:ok, 56, <<>>} = resume_fn.(<<0>>)

      assert {:resume, resume_fn} = Extractors.extract(<<56, 0, 0>>, :i32)
      assert {:ok, 56, <<>>} = resume_fn.(<<0>>)

      assert {:resume, resume_fn} = Extractors.extract(<<56>>, :i16)
      assert {:ok, 56, <<>>} = resume_fn.(<<0>>)

      assert {:resume, resume_fn} = Extractors.extract(<<>>, :i8)
      assert {:ok, 56, <<>>} = resume_fn.(<<56>>)
    end

    test "it should resume lists of things" do
      encoded = [1, 2, 3, 4, 5]
      s = encode({:list, :i64}, encoded)
      <<a, b, c, rest::binary>> = s
      assert {:resume, resume_fn} = Extractors.extract(<<a, b, c>>, {:list, :i64})
      assert {:ok, encoded, <<>>} == resume_fn.(rest)

      assert {:resume, resume_fn} = Extractors.extract(<<>>, {:list, :i64})
      assert {:ok, encoded, <<>>} == resume_fn.(s)
    end

    test "it should be able to resume a nullable" do
      s = <<nil_bit, rest::binary>> = encode({:nullable, :i32}, 15)
      assert {:resume, resume_fn} = Extractors.extract(<<nil_bit>>, {:nullable, :i32})
      assert {:ok, 15, <<>>} = resume_fn.(rest)

      assert {:resume, resume_fn} = Extractors.extract(<<>>, {:nullable, :i32})
      assert {:ok, 15, <<>>} = resume_fn.(s)
    end
  end

  describe "extracting scalar types" do
    test "it should be able to extract signed ints" do
      for type <- ~w(i64 i32 i16 i8)a,
          val = :rand.uniform(127) do
        s = encode(type, val)
        assert {:ok, val, <<>>} == Extractors.extract(s, type)
      end
    end

    test "it should be able to extract unsigned ints" do
      for type <- ~w(u64 u32 u16 u8)a, val = :rand.uniform(127) do
        s = encode(type, val)
        assert {:ok, val, <<>>} == Extractors.extract(s, type)
      end
    end

    test "it should be able to extract strings" do
      s = encode(:string, "hello")
      assert {:ok, "hello", <<>>} = Extractors.extract(s, :string)
    end

    test "it should be able to extract booleans" do
      s = encode(:boolean, true)
      assert {:ok, true, <<>>} = Extractors.extract(s, :boolean)
    end

    test "it should be able to extract floats" do
      for type <- ~w(f64 f32)a do
        s = encode(type, 0.24)
        assert {:ok, val, <<>>} = Extractors.extract(s, type)
      end
    end
  end

  describe "extracting nullable values" do
    test "it should be able to extract a null int" do
      for base_type <- ~w(i64 i32 i16 i8 u64 u32 u16 u8)a,
          type = {:nullable, base_type} do
        val = :rand.uniform(127)
        s = encode(type, val)
        assert {:ok, val, <<>>} = Extractors.extract(s, type)

        s = encode(type, nil)
        assert {:ok, nil, <<>>} = Extractors.extract(s, type)
      end
    end

    test "it should be able to extract nullable strings" do
      for val <- ["hello", nil],
          type = {:nullable, :string} do
        s = encode(type, val)
        assert {:ok, ^val, <<>>} = Extractors.extract(s, type)
      end
    end

    test "it should be able to extract nullable booleans" do
      for val <- [true, false, nil],
          type = {:nullable, :boolean} do
        s = encode(type, val)
        assert {:ok, ^val, <<>>} = Extractors.extract(s, type)
      end
    end

    test "it should be able to extract nullable floats" do
      for base_type <- ~w(f64 f32)a,
          type = {:nullable, base_type} do
        for val <- [32.0, nil] do
          s = encode(type, val)
          assert {:ok, val, <<>>} == Extractors.extract(s, type)
        end
      end
    end
  end

  describe "lists" do
    test "it should be able to extract a list of ints" do
      for base_type <- ~w(i64 i32 i16 i8)a,
          list_type = {:list, base_type} do
        values = 1..10 |> Enum.to_list()
        s = encode(list_type, values)
        assert {:ok, values, <<>>} == Extractors.extract(s, list_type)
      end
    end

    test "it should be able to extract strings" do
      values = ~w(hi there people)
      s = encode({:list, :string}, values)
      assert {:ok, values, <<>>} == Extractors.extract(s, {:list, :string})
    end
  end

  describe "lists with nullable elements" do
    test "it should be able to extract nullable ints" do
      for base_type <- ~w(i64 i32 i16 i8 u64 u32 u16 u8)a,
          list_type = {:list, {:nullable, base_type}} do
        values = [:random.uniform(127), nil, :random.uniform(127), nil]
        s = encode(list_type, values)
        assert {:ok, values, <<>>} == Extractors.extract(s, list_type)
      end
    end
  end
end
