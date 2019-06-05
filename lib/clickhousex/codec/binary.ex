defmodule Clickhousex.Codec.Binary do
  @compile {:bin_opt_info, true}
  use Bitwise

  def encode(:varint, num) when num < 128, do: <<num>>
  def encode(:varint, num), do: <<1::1, num::7, encode(:varint, num >>> 7)::binary>>

  def encode(:string, str) when is_bitstring(str) do
    [encode(:varint, byte_size(str)), str]
  end

  def encode(:u8, i) when is_integer(i) do
    <<i::little-unsigned-size(8)>>
  end

  def encode(:u16, i) do
    <<i::little-unsigned-size(16)>>
  end

  def encode(:u32, i) do
    <<i::little-unsigned-size(32)>>
  end

  def encode(:u64, i) do
    <<i::little-unsigned-size(64)>>
  end

  def encode(:i8, i) do
    <<i::little-signed-size(8)>>
  end

  def encode(:i16, i) do
    <<i::little-signed-size(16)>>
  end

  def encode(:i32, i) do
    <<i::little-signed-size(32)>>
  end

  def encode(:i64, i) do
    <<i::little-signed-size(64)>>
  end

  def encode(:boolean, true) do
    encode(:u8, 1)
  end

  def encode(:boolean, false) do
    encode(:u8, 0)
  end

  def decode(bytes, {:nullable, type}) do
    case decode(bytes, :u8) do
      {:ok, 0, rest} -> decode(rest, type)
      {:ok, 1, rest} -> {:ok, nil, rest}
    end
  end

  def decode(bytes, :struct, struct_module) do
    decode_struct(bytes, struct_module.decode_spec(), struct(struct_module))
  end

  def decode(bytes, :varint) do
    decode_varint(bytes, 0, 0)
  end

  def decode(bytes, :string) do
    with {:ok, byte_count, rest} <- decode(bytes, :varint),
         true <- byte_size(rest) >= byte_count do
      <<decoded_str::binary-size(byte_count), rest::binary>> = rest
      {:ok, decoded_str, rest}
    end
  end

  def decode(<<1::little-unsigned-size(8), rest::binary>>, :boolean) do
    {:ok, true, rest}
  end

  def decode(<<0::little-unsigned-size(8), rest::binary>>, :boolean) do
    {:ok, false, rest}
  end

  def decode(bytes, {:list, data_type}) do
    {:ok, count, rest} = decode(bytes, :varint)
    decode_list(rest, data_type, count, [])
  end

  def decode(<<decoded::little-signed-size(64), rest::binary>>, :i64) do
    {:ok, decoded, rest}
  end

  def decode(<<decoded::little-signed-size(32), rest::binary>>, :i32) do
    {:ok, decoded, rest}
  end

  def decode(<<decoded::little-signed-size(16), rest::binary>>, :i16) do
    {:ok, decoded, rest}
  end

  def decode(<<decoded::little-signed-size(8), rest::binary>>, :i8) do
    {:ok, decoded, rest}
  end

  def decode(<<decoded::little-signed-size(64), rest::binary>>, :u64) do
    {:ok, decoded, rest}
  end

  def decode(<<decoded::little-signed-size(32), rest::binary>>, :u32) do
    {:ok, decoded, rest}
  end

  def decode(<<decoded::little-signed-size(16), rest::binary>>, :u16) do
    {:ok, decoded, rest}
  end

  def decode(<<decoded::little-signed-size(8), rest::binary>>, :u8) do
    {:ok, decoded, rest}
  end

  def decode(<<days_since_epoch::little-unsigned-size(16), rest::binary>>, :date) do
    {:ok, date} = Date.new(1970, 01, 01)
    date = Date.add(date, days_since_epoch)

    {:ok, date, rest}
  end

  def decode(<<seconds_since_epoch::little-unsigned-size(32), rest::binary>>, :datetime) do
    {:ok, date_time} = NaiveDateTime.new(1970, 1, 1, 0, 0, 0)
    date_time = NaiveDateTime.add(date_time, seconds_since_epoch)

    {:ok, date_time, rest}
  end

  def decode(<<0, rest::binary>>, :boolean) do
    {:ok, false, rest}
  end

  def decode(<<1, rest::binary>>, :boolean) do
    {:ok, true, rest}
  end

  def decode(<<decoded::little-signed-float-size(64), rest::binary>>, :f64) do
    {:ok, decoded, rest}
  end

  def decode(<<decoded::little-signed-float-size(32), rest::binary>>, :f32) do
    {:ok, decoded, rest}
  end

  defp decode_list(rest, _, 0, accum) do
    {:ok, Enum.reverse(accum), rest}
  end

  defp decode_list(bytes, data_type, count, accum) do
    case decode(bytes, data_type) do
      {:ok, decoded, rest} -> decode_list(rest, data_type, count - 1, [decoded | accum])
      other -> other
    end
  end

  defp decode_varint(<<0::size(1), byte::size(7), rest::binary>>, result, shift) do
    {:ok, result ||| byte <<< shift, rest}
  end

  defp decode_varint(<<1::1, byte::7, rest::binary>>, result, shift) do
    decode_varint(rest, result ||| byte <<< shift, shift + 7)
  end

  defp decode_struct(rest, [], struct) do
    {:ok, struct, rest}
  end

  defp decode_struct(rest, [{field_name, type} | specs], struct) do
    case decode(rest, type) do
      {:ok, decoded, rest} ->
        decode_struct(rest, specs, Map.put(struct, field_name, decoded))

      {:error, _} = err ->
        err
    end
  end
end
