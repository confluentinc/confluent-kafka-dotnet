using System.Numerics;
using Avro;
using Xunit;
using ProtoDecimalMessage = Confluent.SchemaRegistry.Serdes.Protobuf.Decimal;

namespace Confluent.SchemaRegistry.Serdes.UnitTests;

public class DecimalConversionTests
{
    // Round-trip proto Decimal <-> BigDecimal, including a 38-digit DECIMAL16-range value
    // that would overflow System.Decimal, proving the BigDecimal path is lossless.
    [Fact]
    public void ProtobufDecimal_BigDecimal_RoundTrip()
    {
        var values = new[]
        {
            new BigDecimal(BigInteger.Parse("1234"), 2),
            new BigDecimal(BigInteger.Parse("-1234"), 2),
            new BigDecimal(BigInteger.Parse("1234567890123456789012345678"), 5),
            new BigDecimal(BigInteger.Parse("-1234567890123456789012345678"), 5)
        };

        foreach (var value in values)
        {
            ProtoDecimalMessage proto = value.ToProtobufDecimal();
            BigDecimal result = proto.ToBigDecimal();

            Assert.Equal(value.Unscaled, result.Unscaled);
            Assert.Equal(value.Scale, result.Scale);
        }
    }

    // Round-trip AvroDecimal <-> BigDecimal, including a 38-digit DECIMAL16-range value.
    [Fact]
    public void AvroDecimal_BigDecimal_RoundTrip()
    {
        var values = new[]
        {
            new BigDecimal(BigInteger.Parse("1234"), 2),
            new BigDecimal(BigInteger.Parse("-1234"), 2),
            new BigDecimal(BigInteger.Parse("1234567890123456789012345678"), 5),
            new BigDecimal(BigInteger.Parse("-1234567890123456789012345678"), 5)
        };

        foreach (var value in values)
        {
            AvroDecimal avro = value.ToAvroDecimal();
            BigDecimal result = avro.ToBigDecimal();

            Assert.Equal(value.Unscaled, result.Unscaled);
            Assert.Equal(value.Scale, result.Scale);
        }
    }

    // A large value round-trips losslessly through the BigDecimal-based proto/avro bridges,
    // and a value whose integer part exceeds System.Decimal's range overflows on ToDecimal -
    // exactly the case the BigDecimal path exists to preserve.
    [Fact]
    public void BigDecimalPath_IsLossless_WhereSystemDecimalOverflows()
    {
        var value = new BigDecimal(BigInteger.Parse("1234567890123456789012345678"), 5);

        Assert.Equal(value.Unscaled, value.ToProtobufDecimal().ToBigDecimal().Unscaled);
        Assert.Equal(value.Scale, value.ToProtobufDecimal().ToBigDecimal().Scale);
        Assert.Equal(value.Unscaled, value.ToAvroDecimal().ToBigDecimal().Unscaled);
        Assert.Equal(value.Scale, value.ToAvroDecimal().ToBigDecimal().Scale);

        // A 38-digit unscaled value has a 33-digit integer part, well beyond System.Decimal.
        var tooBig = new BigDecimal(BigInteger.Parse("12345678901234567890123456789012345678"), 5);
        Assert.Equal(tooBig.Unscaled, tooBig.ToProtobufDecimal().ToBigDecimal().Unscaled);
        Assert.Equal(tooBig.Unscaled, tooBig.ToAvroDecimal().ToBigDecimal().Unscaled);
        Assert.Throws<System.OverflowException>(() => tooBig.ToDecimal());
    }

    // The System.Decimal-based extension methods still behave as before on a small value.
    [Fact]
    public void SystemDecimalExtensions_StillRoundTrip()
    {
        decimal input = 12.34m;
        Assert.Equal(input, input.ToProtobufDecimal().ToSystemDecimal());
    }
}
