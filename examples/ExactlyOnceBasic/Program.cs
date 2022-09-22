using System;
using Microsoft.Extensions.Configuration;
using Confluent.Kafka;


/// <remarks>
///     Set Sasl credentials by adding to appsettings.json or via the env vars:
///     KAFKA_EXAMPLE__Common__SaslUsername
///     KAFKA_EXAMPLE__Common__SaslPassword
/// </remarks>
class ProcessConfig
{
    public static T GetConfig<T>(string configSection)
    {
        IConfiguration configuration = new ConfigurationBuilder()
            .AddJsonFile("./appsettings.json")
            .AddEnvironmentVariables("KAFKA_EXAMPLE__")
            .Build();
        var cConfig = configuration.GetSection("Common").Get<T>();
        configuration.GetSection(configSection).Bind(cConfig);
        return cConfig;
    }

    public static AdminClientConfig AdminClientConfig
        => GetConfig<AdminClientConfig>("AdminClient");

    public static ProducerConfig SourceProducerConfig
        => GetConfig<ProducerConfig>("SourceProducer");

    public static ProducerConfig TargetProducerConfig
    {
        get
        {
            var result = GetConfig<ProducerConfig>("TargetProducer");
            result.TransactionalId += "-" + Guid.NewGuid().ToString();
            return result;
        }
    }

    public static ConsumerConfig ConsumerConfig
    {
        get
        {
            var result = GetConfig<ConsumerConfig>("ConsumerConfig");
            result.MaxPollIntervalMs = result.SessionTimeoutMs;
            return result;
        }
    }
}
