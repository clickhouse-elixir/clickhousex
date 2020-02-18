defmodule Clickhousex.Codec.BinaryTest do
  use ExUnit.Case
  import Clickhousex.Codec.Binary

  test "resuming" do
    start = <<1, 0, 0>>
    assert {:resume, resumer} = decode(start, :i32)
    assert {:ok, 1, <<>>} = resumer.(<<0>>)
  end

  test "resuming a varint" do
    expected = decode(<<129, 0>>, :varint)
    start = <<129>>

    assert {:resume, resumer} = decode(start, :varint)
    assert expected == resumer.(<<0>>)

    expected = decode(<<140, 202, 192, 6>>, :varint)
    start = <<140, 202>>

    assert {:resume, resumer} = decode(start, :varint)
    assert expected == resumer.(<<192, 6>>)
  end

  test "decoding an i64" do
    expected = 48_291_928

    encoded = encode(:i64, expected)

    <<start::2-bytes, rest::binary>> = encoded
    assert {:resume, resumer} = decode(start, :i64)
    assert {:ok, expected, <<>>} == resumer.(rest)
  end

  test "resuming a string" do
    to_encode = String.duplicate("hi", 400)
    encoded = encode(:string, to_encode) |> IO.iodata_to_binary()
    <<first_byte::1-bytes, rest::binary>> = encoded

    assert {:resume, resumer} = decode(first_byte, :string)
    assert {:ok, ^to_encode, <<>>} = resumer.(rest)

    <<length::2-bytes, rest::binary>> = encoded

    assert {:resume, resumer} = decode(length, :string)
    assert {:ok, ^to_encode, <<>>} = resumer.(rest)

    <<start::52-bytes, rest::binary>> = encoded
    assert {:resume, resumer} = decode(start, :string)
    assert {:ok, ^to_encode, <<>>} = resumer.(rest)
  end

  test "decoding a list of integers" do
    to_encode = 1..300 |> Enum.to_list()

    for type <- ~w(u64 u32 u16 i64 i32 i16)a do
      encoded = encode({:list, type}, to_encode) |> IO.iodata_to_binary()
      <<first_byte::1-bytes, rest::binary>> = encoded

      assert {:resume, resumer} = decode(first_byte, {:list, type})
      assert {:ok, to_encode, <<>>} == resumer.(rest)

      <<length::2-bytes, rest::binary>> = encoded
      assert {:resume, resumer} = decode(length, {:list, type})
      assert {:ok, to_encode, <<>>} == resumer.(rest)

      <<first::100-bytes, rest::binary>> = encoded
      assert {:resume, resumer} = decode(first, {:list, type})
      assert {:ok, to_encode, <<>>} == resumer.(rest)
    end
  end

  test "decoding a list of floats" do
    to_encode = Enum.map(1..300, &(&1 / 1))

    for type <- ~w(f64 f32 )a do
      encoded = encode({:list, type}, to_encode) |> IO.iodata_to_binary()
      <<first_byte::1-bytes, rest::binary>> = encoded

      assert {:resume, resumer} = decode(first_byte, {:list, type})
      assert {:ok, to_encode, <<>>} == resumer.(rest)

      <<length::2-bytes, rest::binary>> = encoded
      assert {:resume, resumer} = decode(length, {:list, type})
      assert {:ok, to_encode, <<>>} == resumer.(rest)

      <<first::100-bytes, rest::binary>> = encoded
      assert {:resume, resumer} = decode(first, {:list, type})
      assert {:ok, to_encode, <<>>} == resumer.(rest)
    end
  end

  test "decoding a nullable string" do
    null = encode({:nullable, :string}, nil) |> IO.iodata_to_binary()
    assert {:resume, resumer} = decode(<<>>, {:nullable, :string})
    assert {:ok, long_string, <<>>} = resumer.(null)

    long_string = String.duplicate("h", 300)
    non_null = encode({:nullable, :string}, long_string) |> IO.iodata_to_binary()

    <<start::1-bytes, rest::binary>> = non_null
    assert {:resume, resumer} = decode(start, {:nullable, :string})
    assert {:ok, long_string, <<>>} == resumer.(rest)

    <<start::2-bytes, rest::binary>> = non_null
    assert {:resume, resumer} = decode(start, {:nullable, :string})
    assert {:ok, long_string, <<>>} == resumer.(rest)

    <<start::128-bytes, rest::binary>> = non_null
    assert {:resume, resumer} = decode(start, {:nullable, :string})
    assert {:ok, long_string, <<>>} == resumer.(rest)
  end
end
