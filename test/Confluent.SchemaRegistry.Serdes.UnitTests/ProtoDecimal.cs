using Xunit;

namespace Confluent.SchemaRegistry.Serdes.UnitTests;

public class ProtoDecimal
{
    [Fact]
    public void ConvertDecimals()
    {
        var inputs = new[]
        {
            0m,
            1.01m,
            123456789123456789.56m,
            1234m,
            1234.5m,
            -0m,
            -1.01m,
            -123456789123456789.56m,
            -1234m,
            -1234.5m,
            -1234.56m
        };

        foreach (var input in inputs)
        {
            var converted = input.ToProtobufDecimal();
            var original = converted.ToSystemDecimal();

            Assert.Equal(input, original);
        }
    }
}