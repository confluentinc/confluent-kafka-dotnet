// Copyright 2026 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.

using System;
using System.Collections.Generic;
using System.Threading;
using Confluent.Kafka;
using Confluent.Kafka.OAuthBearer.Aws;

/// <summary>
///     Produces and consumes a message against a Kafka broker that accepts
///     OAUTHBEARER tokens minted by AWS STS <c>GetWebIdentityToken</c>.
///
///     The broker's OIDC trust is configured to accept tokens whose issuer is
///     the caller's AWS account — see the AWS IAM "Outbound Identity
///     Federation" documentation. This example does not configure that; it
///     only shows the client side.
///
///     Prerequisites (all admin actions, not the client's concern):
///       * The AWS account has run
///           aws iam enable-outbound-web-identity-federation
///         once.
///       * The IAM identity the caller runs under has
///           sts:GetWebIdentityToken
///         permission.
///       * The broker trusts the AWS-hosted JWKS for the account's issuer.
///
///     Run with:
///         dotnet run -- &lt;bootstrap&gt; &lt;topic&gt; &lt;group&gt; &lt;region&gt; &lt;audience&gt;
///
///     Example:
///         dotnet run -- pkc-xxxxx.us-east-1.aws.confluent.cloud:9092 \
///                       my-topic my-group us-east-1 https://confluent.cloud/oidc
/// </summary>
namespace Confluent.Kafka.Examples.OAuthBearerAws
{
    public class Program
    {
        public static void Main(string[] args)
        {
            if (args.Length != 5)
            {
                Console.WriteLine(
                    "Usage: OAuthBearerAws <bootstrap> <topic> <group> <region> <audience>");
                Environment.Exit(1);
            }
            var bootstrapServers = args[0];
            var topic = args[1];
            var groupId = args[2];
            var region = args[3];
            var audience = args[4];

            var commonConfig = new ClientConfig
            {
                BootstrapServers = bootstrapServers,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.OAuthBearer,
                // NOTE: do NOT set SaslOauthbearerMethod here — that selects
                // the librdkafka-native OIDC path and bypasses our managed
                // refresh handler.
            };
            var producerConfig = new ProducerConfig(commonConfig);
            var consumerConfig = new ConsumerConfig(commonConfig)
            {
                GroupId = groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
            };

            // The AWS config and provider are shared between the producer and
            // consumer. Constructing one provider keeps a single STS client
            // (one HTTP connection pool, one credential cache) instead of
            // duplicating it per Kafka client.
            var awsConfig = new AwsOAuthBearerConfig
            {
                Region = region,
                Audience = audience,
                Duration = TimeSpan.FromHours(1),
                SaslExtensions = new Dictionary<string, string>
                {
                    // Typical Confluent Cloud extensions — edit to match your broker.
                    // ["logicalCluster"] = "lkc-xxxxx",
                    // ["identityPoolId"] = "pool-xxxxx",
                },
            };

            using var provider = new AwsStsTokenProvider(awsConfig);

            using var producer = new ProducerBuilder<Null, string>(producerConfig)
                .UseAwsOAuthBearer(provider)
                .Build();

            using var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig)
                .UseAwsOAuthBearer(provider)
                .Build();

            consumer.Subscribe(topic);

            using var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

            // Produce one message, then consume it.
            try
            {
                var deliveryReport = producer
                    .ProduceAsync(topic, new Message<Null, string> { Value = "hello from AWS IAM" })
                    .GetAwaiter().GetResult();
                Console.WriteLine($"Produced to {deliveryReport.TopicPartitionOffset}");
            }
            catch (ProduceException<Null, string> e)
            {
                Console.WriteLine($"Produce failed: {e.Error.Reason}");
                Environment.Exit(1);
            }

            try
            {
                while (!cts.IsCancellationRequested)
                {
                    try
                    {
                        var cr = consumer.Consume(cts.Token);
                        Console.WriteLine(
                            $"Consumed '{cr.Message.Value}' from {cr.TopicPartitionOffset}");
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Consume error: {e.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                consumer.Close();
            }
        }
    }
}
