using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using PublicApiGenerator;
using Shouldly;
using System;
using Xunit;

namespace Confluent.Kafka.ApprovalTests
{
    public class ApiApprovalTests
    {
        /// <summary> Check for changes to the public APIs. </summary>
        /// <param name="type"> The type used as a marker for the assembly whose public API change you want to check. </param>
        [Theory]
        [InlineData(typeof(Message<,>))]
        [InlineData(typeof(SchemaRegistryConfig))]
        [InlineData(typeof(AvroSerializer<>))]
        [InlineData(typeof(JsonSerializer<>))]
        [InlineData(typeof(ProtobufSerializer<>))]
        public void PublicApi(Type type)
        {
            string publicApi = type.Assembly.GeneratePublicApi(new ApiGeneratorOptions
            {
                IncludeAssemblyAttributes = false,
                ExcludeAttributes = new[] { "System.Diagnostics.DebuggerDisplayAttribute" },
            });

            publicApi.ShouldMatchApproved(options => options!.WithFilenameGenerator((testMethodInfo, discriminator, fileType, fileExtension) => $"{type.Assembly.GetName().Name!}.{fileType}.{fileExtension}"));
        }
    }
}
