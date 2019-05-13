using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace Confluent.Kafka.UnitTests
{
  public class FilterByHeaderProcessorTests
  {
    [Fact]
    public void NoFilter_NoMessageKept()
    {
      var processor = new FilterByHeaderProcessor(new List<FilterByHeader>());
      Assert.False(processor.ShouldKeepMessage(new Headers()));
    }

    [Fact]
    public void KeyNotPresentInHeaders_KeepMessageIfKeyNotPresent_MessageKept()
    {
      var filter = new FilterByHeader { Key = "My_key", KeepMessageIfHeaderNotDefined = true, ShouldKeepMessage = _ => false };
      var processor = new FilterByHeaderProcessor(new[] { filter });
      Assert.True(processor.ShouldKeepMessage(new Headers()));
    }

    [Fact]
    public void KeyNotPresentInHeaders_DoNotKeepMessageIfKeyNotPresent_NoMessageKept()
    {
      var filter = new FilterByHeader { Key = "My_key", KeepMessageIfHeaderNotDefined = false, ShouldKeepMessage = _ => false };
      var processor = new FilterByHeaderProcessor(new[] { filter });
      Assert.False(processor.ShouldKeepMessage(new Headers()));
    }

    [Fact]
    public void KeyPresentInHeaders_MessageKept()
    {
      const string key = "Pitming";

      var headers = new Headers
      {
        { key, Encoding.UTF8.GetBytes("is my lord") }
      };

      var filter = new FilterByHeader { Key = key, KeepMessageIfHeaderNotDefined = false, ShouldKeepMessage = _ => true };

      var processor = new FilterByHeaderProcessor(new[] { filter });

      Assert.True(processor.ShouldKeepMessage(headers));
    }

    [Fact]
    public void FilterThrow_NoMessageKept()
    {
      const string key = "Pitming";

      var headers = new Headers
      {
        { key, Encoding.UTF8.GetBytes("is my lord") }
      };

      var filter = new FilterByHeader
      {
        Key = key,
        KeepMessageIfHeaderNotDefined = false,
        ShouldKeepMessage = _ => throw new Exception()
      };

      var processor = new FilterByHeaderProcessor(new[] { filter });

      Assert.False(processor.ShouldKeepMessage(headers));
    }

    [Fact]
    public void KeyPresentInHeaders_ValueMatch_MessageKept()
    {
      const string key = "Pitming";
      const string value = "is my lord";

      var headers = new Headers
      {
        { key, Encoding.UTF8.GetBytes(value) }
      };

      var filter = new FilterByHeader
      {
        Key = key,
        KeepMessageIfHeaderNotDefined = false,
        ShouldKeepMessage = header =>
        {
          var headerValue = Encoding.UTF8.GetString(header.GetValueBytes());
          return string.Compare(value, headerValue) == 0;
        }
      };

      var processor = new FilterByHeaderProcessor(new[] { filter });

      Assert.True(processor.ShouldKeepMessage(headers));
    }

    [Fact]
    public void OneFilterMatch_AnotherDoNotMatch_MessageKept()
    {
      const string key = "Pitming";
      const string value = "is my lord";

      var headers = new Headers
      {
        { key, Encoding.UTF8.GetBytes(value) }
      };

      var filter1 = new FilterByHeader
      {
        Key = key,
        KeepMessageIfHeaderNotDefined = false,
        ShouldKeepMessage = header =>
        {
          var headerValue = Encoding.UTF8.GetString(header.GetValueBytes());
          return string.Compare(value, headerValue) == 0;
        }
      };

      var filter2 = new FilterByHeader
      {
        Key = key,
        ShouldKeepMessage = _ => false
      };

      var processor = new FilterByHeaderProcessor(new[] { filter1, filter2 });

      Assert.True(processor.ShouldKeepMessage(headers));
    }

    [Fact]
    public void OneFilterThrow_AnotherMatch_MessageKept()
    {
      const string key = "Pitming";
      const string value = "is my lord";

      var headers = new Headers
      {
        { key, Encoding.UTF8.GetBytes(value) }
      };

      var filter1 = new FilterByHeader
      {
        Key = key,

        ShouldKeepMessage = _ => throw new Exception()
      };

      var filter2 = new FilterByHeader
      {
        Key = key,
        ShouldKeepMessage = _ => true
      };

      var processor = new FilterByHeaderProcessor(new[] { filter1, filter2 });

      Assert.True(processor.ShouldKeepMessage(headers));
    }
  }
}
