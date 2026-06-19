using System;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using Xunit;

namespace Confluent.Kafka.IntegrationTests.Raw;

[Collection(KafkaCollection.Name)]
public class Producer_RawProduceTests
{
    private readonly KafkaFixture kafka;

    public Producer_RawProduceTests(KafkaFixture kafka)
    {
        this.kafka = kafka;
    }

    private IRawConsumer BuildConsumer(string topic)
    {
        var consumer = new RawConsumerBuilder(new ConsumerConfig
        {
            BootstrapServers = kafka.BootstrapServers,
            GroupId = Guid.NewGuid().ToString(),
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        }).BuildRaw();
        consumer.Subscribe(topic);
        return consumer;
    }

    [Fact]
    public void Produce_RoundTrip_BytesMatch()
    {
        var topic = $"raw-prod-{Guid.NewGuid():N}";
        var key = Encoding.UTF8.GetBytes("k");
        var value = Encoding.UTF8.GetBytes("hello-value");

        using (var producer = new RawProducerBuilder(new ProducerConfig
        {
            BootstrapServers = kafka.BootstrapServers,
            Acks = Acks.All,
        }).BuildRaw())
        {
            producer.RawProduce(topic, key, value);
            producer.Flush(TimeSpan.FromSeconds(10));
        }

        using var consumer = BuildConsumer(topic);
        using var msg = consumer.ConsumeRaw(TimeSpan.FromSeconds(30));

        Assert.False(msg.IsEmpty);
        Assert.Equal(ErrorCode.NoError, msg.ErrorCode);
        Assert.True(msg.Key.SequenceEqual(key));
        Assert.True(msg.Value.SequenceEqual(value));
    }

    [Fact]
    public void Produce_WithPartition_LandsOnThatPartition()
    {
        var topic = $"raw-prod-part-{Guid.NewGuid():N}";

        using (var producer = new RawProducerBuilder(new ProducerConfig
        {
            BootstrapServers = kafka.BootstrapServers,
            Acks = Acks.All,
        }).BuildRaw())
        {
            producer.RawProduce(topic, partition: new Partition(0), default, Encoding.UTF8.GetBytes("v"));
            producer.Flush(TimeSpan.FromSeconds(10));
        }

        using var consumer = BuildConsumer(topic);
        using var msg = consumer.ConsumeRaw(TimeSpan.FromSeconds(30));

        Assert.False(msg.IsEmpty);
        Assert.Equal(0, (int)msg.Partition);
    }

    [Fact]
    public void Produce_WithHeaders_RoundTrip()
    {
        var topic = $"raw-prod-hdr-{Guid.NewGuid():N}";
        var value = Encoding.UTF8.GetBytes("v");
        var hval1 = Encoding.UTF8.GetBytes("one");
        var hval2 = Encoding.UTF8.GetBytes("two");

        var headers = new KafkaHeaders();
        headers.Add("h1", hval1);
        headers.Add("h2", hval2);

        using (var producer = new RawProducerBuilder(new ProducerConfig
        {
            BootstrapServers = kafka.BootstrapServers,
            Acks = Acks.All,
        }).BuildRaw())
        {
            producer.RawProduce(topic, default, value, in headers);
            producer.Flush(TimeSpan.FromSeconds(10));
        }

        using var consumer = BuildConsumer(topic);
        using var msg = consumer.ConsumeRaw(TimeSpan.FromSeconds(30));

        Assert.False(msg.IsEmpty);
        Assert.False(msg.Headers.IsEmpty);

        int index = 0;
        foreach (var (name, val) in msg.Headers)
        {
            switch (index++)
            {
                case 0:
                    Assert.True(name.SequenceEqual("h1"u8));
                    Assert.True(val.SequenceEqual(hval1));
                    break;
                case 1:
                    Assert.True(name.SequenceEqual("h2"u8));
                    Assert.True(val.SequenceEqual(hval2));
                    break;
            }
        }
        Assert.Equal(2, index);
    }

    [Fact]
    public void Produce_WithTwoHeaderSets_Concatenated_RoundTrip()
    {
        var topic = $"raw-prod-hdr2-{Guid.NewGuid():N}";
        var value = Encoding.UTF8.GetBytes("v");
        var hval1 = Encoding.UTF8.GetBytes("one");
        var hval2 = Encoding.UTF8.GetBytes("two");
        var hval3 = Encoding.UTF8.GetBytes("three");

        var shared = new KafkaHeaders();
        shared.Add("h1", hval1);
        shared.Add("h2", hval2);

        var perMessage = new KafkaHeaders();
        perMessage.Add("h3", hval3);

        using (var producer = new RawProducerBuilder(new ProducerConfig
        {
            BootstrapServers = kafka.BootstrapServers,
            Acks = Acks.All,
        }).BuildRaw())
        {
            producer.RawProduce(topic, default, value, in shared, in perMessage);
            producer.Flush(TimeSpan.FromSeconds(10));
        }

        using var consumer = BuildConsumer(topic);
        using var msg = consumer.ConsumeRaw(TimeSpan.FromSeconds(30));

        Assert.False(msg.IsEmpty);
        Assert.False(msg.Headers.IsEmpty);

        int index = 0;
        foreach (var (name, val) in msg.Headers)
        {
            switch (index++)
            {
                case 0:
                    Assert.True(name.SequenceEqual("h1"u8));
                    Assert.True(val.SequenceEqual(hval1));
                    break;
                case 1:
                    Assert.True(name.SequenceEqual("h2"u8));
                    Assert.True(val.SequenceEqual(hval2));
                    break;
                case 2:
                    Assert.True(name.SequenceEqual("h3"u8));
                    Assert.True(val.SequenceEqual(hval3));
                    break;
            }
        }
        Assert.Equal(3, index);
    }

    [Fact]
    public void DeliveryReportHandler_Fires_OnSuccess()
    {
        var topic = $"raw-prod-dr-{Guid.NewGuid():N}";
        var key = Encoding.UTF8.GetBytes("k");
        var value = Encoding.UTF8.GetBytes("v");

        int callCount = 0;
        ErrorCode observedErr = default;
        PersistenceStatus observedStatus = default;
        Partition observedPartition = default;
        Offset observedOffset = default;
        TimestampType observedTsType = default;
        DateTime observedTsUtc = default;
        byte[] observedKey = null;
        byte[] observedValue = null;
        byte[] observedTopic = null;

        var producer = new RawProducerBuilder(new ProducerConfig
        {
            BootstrapServers = kafka.BootstrapServers,
            Acks = Acks.All,
        })
        .SetDeliveryReportHandler((in RawDeliveryReport report) =>
        {
            Interlocked.Increment(ref callCount);
            observedErr = report.ErrorCode;
            observedStatus = report.Status;
            observedPartition = report.Partition;
            observedOffset = report.Offset;
            observedTsType = report.Timestamp.Type;
            observedTsUtc = report.Timestamp.UtcDateTime;
            observedKey = report.Key.ToArray();
            observedValue = report.Value.ToArray();
            observedTopic = report.Topic.ToArray();
        })
        .BuildRaw();

        try
        {
            producer.RawProduce(topic, key, value);
            producer.Flush(TimeSpan.FromSeconds(10));
        }
        finally
        {
            producer.Dispose();
        }

        Assert.Equal(1, callCount);
        Assert.Equal(ErrorCode.NoError, observedErr);
        Assert.Equal(PersistenceStatus.Persisted, observedStatus);
        Assert.Equal((Partition)0, observedPartition);
        Assert.True((long)observedOffset >= 0);
        Assert.Equal(TimestampType.CreateTime, observedTsType);
        Assert.True(Math.Abs((DateTime.UtcNow - observedTsUtc).TotalMinutes) < 1.0);
        Assert.Equal(key, observedKey);
        Assert.Equal(value, observedValue);
        Assert.Equal(Encoding.UTF8.GetBytes(topic), observedTopic);
    }

    [Fact]
    public void ProduceNoCopy_RoundTrip_BytesMatch()
    {
        var topic = $"raw-prod-nocopy-{Guid.NewGuid():N}";
        var key = Encoding.UTF8.GetBytes("k");
        var value = Encoding.UTF8.GetBytes("hello-nocopy");

        // Pinning the buffers so memory is stable until delivery completes.
        var keyHandle = GCHandle.Alloc(key, GCHandleType.Pinned);
        var valHandle = GCHandle.Alloc(value, GCHandleType.Pinned);

        try
        {
            IRawProducer producer = new RawProducerBuilder(new ProducerConfig
            {
                BootstrapServers = kafka.BootstrapServers,
                Acks = Acks.All,
            }).BuildRaw();

            try
            {
                RawProducerMarshal.ProduceNoCopy(
                    ref producer,
                    topic,
                    keyHandle.AddrOfPinnedObject(), key.Length,
                    valHandle.AddrOfPinnedObject(), value.Length);
                producer.Flush(TimeSpan.FromSeconds(10));
            }
            finally
            {
                producer.Dispose();
            }
        }
        finally
        {
            keyHandle.Free();
            valHandle.Free();
        }

        using var consumer = BuildConsumer(topic);
        using var msg = consumer.ConsumeRaw(TimeSpan.FromSeconds(30));

        Assert.False(msg.IsEmpty);
        Assert.True(msg.Key.SequenceEqual(key));
        Assert.True(msg.Value.SequenceEqual(value));
    }
}
