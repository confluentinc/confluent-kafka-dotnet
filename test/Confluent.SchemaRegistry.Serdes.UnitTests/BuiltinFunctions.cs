using System.Text;
using Confluent.SchemaRegistry.Rules;
using Xunit;

namespace Confluent.SchemaRegistry.Serdes.UnitTests;

public class BuiltinFunctions
{
    [Fact]
    public void EmailFailure()
    {
        Assert.False(BuiltinOverload.ValidateEmail("ab.com"));
    }

    [Fact]
    public void EmailSuccess()
    {
        Assert.True(BuiltinOverload.ValidateEmail("a@b.com"));
    }

    [Fact]
    public void HostnameLengthFailure()
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 256; ++i)
        {
            sb.Append('a');
        }

        string subject = sb.ToString();
        Assert.False(BuiltinOverload.ValidateHostname(subject));
    }

    [Fact]
    public void HostnameSuccess()
    {
        Assert.True(BuiltinOverload.ValidateHostname("localhost"));
    }

    [Fact]
    public void Ipv4Failure()
    {
        Assert.False(BuiltinOverload.ValidateIpv4("asd"));
    }

    [Fact]
    public void Ipv4LengthFailure()
    {
        Assert.False(BuiltinOverload.ValidateIpv4("2001:db8:85a3:0:0:8a2e:370:7334"));
    }

    [Fact]
    public void Ipv4Success()
    {
        Assert.True(BuiltinOverload.ValidateIpv4("127.0.0.1"));
    }

    [Fact]
    public void Ipv6Failure()
    {
        Assert.False(BuiltinOverload.ValidateIpv6("asd"));
    }

    [Fact]
    public void Ipv6LengthFailure()
    {
        Assert.False(BuiltinOverload.ValidateIpv6("127.0.0.1"));
    }

    [Fact]
    public void Ipv6Success()
    {
        Assert.True(BuiltinOverload.ValidateIpv6("2001:db8:85a3:0:0:8a2e:370:7334"));
    }

    [Fact]
    public void UriFailure()
    {
        Assert.False(BuiltinOverload.ValidateUri("12 34"));
    }

    [Fact]
    public void RelativeUriFails()
    {
        Assert.False(BuiltinOverload.ValidateUri("example.com"));
    }

    [Fact]
    public void RelativeUriRefFails()
    {
        Assert.False(BuiltinOverload.ValidateUri("abc"));
    }

    [Fact]
    public void UriSuccess()
    {
        Assert.True(BuiltinOverload.ValidateUri("http://example.org:8080/example.html"));
    }

    [Fact]
    public void UriRefSuccess()
    {
        Assert.True(BuiltinOverload.ValidateUriRef("http://foo.bar/?baz=qux#quux"));
    }

    [Fact]
    public void RelativeUriRefSuccess()
    {
        Assert.True(BuiltinOverload.ValidateUriRef("//foo.bar/?baz=qux#quux"));
    }

    [Fact]
    public void PathSuccess()
    {
        Assert.True(BuiltinOverload.ValidateUriRef("/abc"));
    }

    [Fact]
    public void UuidFailure()
    {
        Assert.False(BuiltinOverload.ValidateUuid("97cd-6e3d1bc14494"));
    }

    [Fact]
    public void UuidSuccess()
    {
        Assert.True(BuiltinOverload.ValidateUuid("fa02a430-892f-4160-97cd-6e3d1bc14494"));
    }
}