using System;
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
            -1234.56m,
            4.1748330066797328106875724500m,
            -4.1748330066797328106875724500m
        };

        foreach (var input in inputs)
        {
            var converted = input.ToProtobufDecimal();
            var result = converted.ToSystemDecimal();

            Assert.Equal(input, result);
        }
    }
}