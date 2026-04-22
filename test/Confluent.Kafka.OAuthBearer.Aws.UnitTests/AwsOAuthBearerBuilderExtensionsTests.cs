using System;
using System.Reflection;
using System.Threading.Tasks;
using Amazon.SecurityToken.Model;
using Confluent.Kafka;
using Confluent.Kafka.OAuthBearer.Aws;
using Xunit;

namespace Confluent.Kafka.OAuthBearer.Aws.UnitTests
{
    public class AwsOAuthBearerBuilderExtensionsTests
    {
        // ---- Null-builder guards ----

        [Fact]
        public void UseAwsOAuthBearer_NullProducerBuilder_Throws()
        {
            ProducerBuilder<Null, string> nullBuilder = null;
            Assert.Throws<ArgumentNullException>(
                () => nullBuilder.UseAwsOAuthBearer(NewProvider()));
        }

        [Fact]
        public void UseAwsOAuthBearer_NullConsumerBuilder_Throws()
        {
            ConsumerBuilder<Ignore, string> nullBuilder = null;
            Assert.Throws<ArgumentNullException>(
                () => nullBuilder.UseAwsOAuthBearer(NewProvider()));
        }

        [Fact]
        public void UseAwsOAuthBearer_NullAdminClientBuilder_Throws()
        {
            AdminClientBuilder nullBuilder = null;
            Assert.Throws<ArgumentNullException>(
                () => nullBuilder.UseAwsOAuthBearer(NewProvider()));
        }

        // ---- ProducerBuilder wiring ----

        [Fact]
        public void ProducerBuilder_UseAwsOAuthBearer_InstallsHandler()
        {
            var builder = NewProducerBuilder();
            Assert.Null(GetHandler(builder));

            var returned = builder.UseAwsOAuthBearer(NewProvider());

            Assert.Same(builder, returned); // fluent — returns the builder
            Assert.NotNull(GetHandler(builder));
        }

        [Fact]
        public void ProducerBuilder_UseAwsOAuthBearer_CalledTwice_Throws()
        {
            var builder = NewProducerBuilder().UseAwsOAuthBearer(NewProvider());
            // Second call must fail — Confluent.Kafka's SetOAuthBearerTokenRefreshHandler
            // rejects double-installation.
            Assert.Throws<InvalidOperationException>(
                () => builder.UseAwsOAuthBearer(NewProvider()));
        }

        [Fact]
        public void ProducerBuilder_UseAwsOAuthBearer_WithConfigOverload_InstallsHandler()
        {
            var builder = NewProducerBuilder();
            builder.UseAwsOAuthBearer(new AwsOAuthBearerConfig
            {
                Region = "us-east-1",
                Audience = "https://a",
            });
            Assert.NotNull(GetHandler(builder));
        }

        // ---- ConsumerBuilder wiring ----

        [Fact]
        public void ConsumerBuilder_UseAwsOAuthBearer_InstallsHandler()
        {
            var builder = NewConsumerBuilder();
            Assert.Null(GetHandler(builder));

            builder.UseAwsOAuthBearer(NewProvider());
            Assert.NotNull(GetHandler(builder));
        }

        [Fact]
        public void ConsumerBuilder_UseAwsOAuthBearer_CalledTwice_Throws()
        {
            var builder = NewConsumerBuilder().UseAwsOAuthBearer(NewProvider());
            Assert.Throws<InvalidOperationException>(
                () => builder.UseAwsOAuthBearer(NewProvider()));
        }

        // ---- AdminClientBuilder wiring ----

        [Fact]
        public void AdminClientBuilder_UseAwsOAuthBearer_InstallsHandler()
        {
            var builder = NewAdminClientBuilder();
            Assert.Null(GetHandler(builder));

            builder.UseAwsOAuthBearer(NewProvider());
            Assert.NotNull(GetHandler(builder));
        }

        [Fact]
        public void AdminClientBuilder_UseAwsOAuthBearer_CalledTwice_Throws()
        {
            var builder = NewAdminClientBuilder().UseAwsOAuthBearer(NewProvider());
            Assert.Throws<InvalidOperationException>(
                () => builder.UseAwsOAuthBearer(NewProvider()));
        }

        // ---- Helpers ----

        private static ProducerBuilder<Null, string> NewProducerBuilder()
            => new ProducerBuilder<Null, string>(
                new ProducerConfig { BootstrapServers = "localhost:9092" });

        private static ConsumerBuilder<Ignore, string> NewConsumerBuilder()
            => new ConsumerBuilder<Ignore, string>(
                new ConsumerConfig { BootstrapServers = "localhost:9092", GroupId = "g" });

        private static AdminClientBuilder NewAdminClientBuilder()
            => new AdminClientBuilder(
                new AdminClientConfig { BootstrapServers = "localhost:9092" });

        private static AwsStsTokenProvider NewProvider()
            => new AwsStsTokenProvider(
                new AwsOAuthBearerConfig { Region = "us-east-1", Audience = "https://a" },
                new FakeStsClient((req, ct) =>
                    Task.FromResult(new GetWebIdentityTokenResponse())));

        // Reads the builder's OAuthBearerTokenRefreshHandler property. On
        // ProducerBuilder/ConsumerBuilder it's `internal protected`; on
        // AdminClientBuilder it's `public`. Include both binding flags so the
        // one lookup works for all three.
        private static Delegate GetHandler(object builder)
        {
            var prop = builder.GetType().GetProperty(
                "OAuthBearerTokenRefreshHandler",
                BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            Assert.NotNull(prop); // fail loudly if Confluent.Kafka renames it
            return (Delegate)prop.GetValue(builder);
        }
    }
}
