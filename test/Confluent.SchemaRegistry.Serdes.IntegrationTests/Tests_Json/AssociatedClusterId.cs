// Copyright 2025 Confluent Inc.
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
using System.Threading.Tasks;
using Confluent.Kafka;
using Xunit;


namespace Confluent.SchemaRegistry.Serdes.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     The Associated subject name strategy resolves a subject by asking
        ///     Schema Registry which subject is associated with the topic, scoped to
        ///     the id of the Kafka cluster the client is connected to.
        ///
        ///     This test proves that the cluster id is resolved from the connected
        ///     cluster and reaches the serializer, with no
        ///     subject.name.strategy.kafka.cluster.id configured.
        ///
        ///     Note that the association is registered against a subject name the
        ///     Topic fallback could never produce. Without that, the test would pass
        ///     even where associations are unsupported: an association lookup that
        ///     404s is swallowed, and the strategy quietly falls back to
        ///     "&lt;topic&gt;-value".
        /// </summary>
        [Theory, MemberData(nameof(TestParameters))]
        public static async Task AssociatedClusterIdJson(string bootstrapServers, string schemaRegistryServers)
        {
            var schemaRegistryConfig = new SchemaRegistryConfig { Url = schemaRegistryServers };
            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };

            string clusterId;
            using (var adminClient = new AdminClientBuilder(
                new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                clusterId = adminClient.ClusterId(TimeSpan.FromSeconds(30));
            }

            Assert.False(string.IsNullOrEmpty(clusterId),
                "the broker did not report a cluster id");

            using (var topic = new TemporaryTopic(bootstrapServers, 1))
            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            {
                // A subject name the Topic fallback cannot generate, so that only a
                // successful association lookup can satisfy the assertion below.
                var associatedSubject = $"assoc-{Guid.NewGuid():N}-subject";

                await SkipUnlessAssociationsAreSupported(schemaRegistry, topic.Name, clusterId);

                var schema = new Schema(
                    NJsonSchema.JsonSchema.FromType<SubjectNameStrategyTestPoco>().ToJson(),
                    SchemaType.Json);
                await schemaRegistry.RegisterSchemaAsync(associatedSubject, schema, true);

                await schemaRegistry.CreateAssociationAsync(new AssociationCreateOrUpdateRequest(
                    resourceName: topic.Name,
                    resourceNamespace: clusterId,
                    resourceId: clusterId + ":" + topic.Name,
                    resourceType: "topic",
                    associations: new List<AssociationCreateOrUpdateInfo>
                    {
                        new AssociationCreateOrUpdateInfo(
                            subject: associatedSubject,
                            associationType: "value",
                            lifecycle: "STRONG",
                            frozen: null,
                            schema: null,
                            normalize: null)
                    }));

                try
                {
                    // No cluster id is configured: the producer must resolve it from
                    // the cluster and hand it to the serializer.
                    using (var producer =
                        new ProducerBuilder<string, SubjectNameStrategyTestPoco>(producerConfig)
                            .SetValueSerializerBuilder(
                                new JsonSerializerBuilder<SubjectNameStrategyTestPoco>()
                                    .SetSchemaRegistryClient(schemaRegistry)
                                    .SetSerializerConfig(new JsonSerializerConfig
                                    {
                                        SubjectNameStrategy = SubjectNameStrategy.Associated,
                                        AutoRegisterSchemas = false,
                                        UseLatestVersion = true
                                    }))
                            .Build())
                    {
                        await producer.ProduceAsync(
                            topic.Name,
                            new Message<string, SubjectNameStrategyTestPoco>
                            {
                                Key = "test1",
                                Value = new SubjectNameStrategyTestPoco { Value = "test-string" }
                            },
                            TestContext.Current.CancellationToken);
                    }

                    // The association was resolved only if the associated subject was
                    // used. Had the cluster id not been propagated, the lookup would
                    // have missed and the Topic fallback would have written to
                    // "<topic>-value" instead.
                    var subjects = await schemaRegistry.GetAllSubjectsAsync();
                    Assert.Contains(associatedSubject, subjects);
                    Assert.DoesNotContain(topic.Name + "-value", subjects);
                }
                finally
                {
                    var associations = await schemaRegistry.GetAssociationsByResourceNameAsync(
                        topic.Name, clusterId, "topic", null, null, 0, -1);
                    if (associations.Count > 0)
                    {
                        await schemaRegistry.DeleteAssociationsAsync(
                            associations[0].ResourceId, "topic",
                            new List<string> { "value" }, true);
                    }
                }
            }
        }

        /// <summary>
        ///     An association registered under the namespace wildcard must not be
        ///     picked up once a real cluster id is in play, since the lookup is
        ///     scoped to the cluster.
        /// </summary>
        [Theory, MemberData(nameof(TestParameters))]
        public static async Task AssociatedClusterIdIsScopedJson(
            string bootstrapServers, string schemaRegistryServers)
        {
            var schemaRegistryConfig = new SchemaRegistryConfig { Url = schemaRegistryServers };
            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };

            string clusterId;
            using (var adminClient = new AdminClientBuilder(
                new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                clusterId = adminClient.ClusterId(TimeSpan.FromSeconds(30));
            }

            using (var topic = new TemporaryTopic(bootstrapServers, 1))
            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            {
                var wildcardSubject = $"assoc-wildcard-{Guid.NewGuid():N}-subject";

                await SkipUnlessAssociationsAreSupported(schemaRegistry, topic.Name, clusterId);

                var schema = new Schema(
                    NJsonSchema.JsonSchema.FromType<SubjectNameStrategyTestPoco>().ToJson(),
                    SchemaType.Json);
                await schemaRegistry.RegisterSchemaAsync(wildcardSubject, schema, true);

                // Registered under the wildcard namespace, not the real cluster id.
                await schemaRegistry.CreateAssociationAsync(new AssociationCreateOrUpdateRequest(
                    resourceName: topic.Name,
                    resourceNamespace: AssociatedNameStrategy.NamespaceWildcard,
                    resourceId: AssociatedNameStrategy.NamespaceWildcard + ":" + topic.Name,
                    resourceType: "topic",
                    associations: new List<AssociationCreateOrUpdateInfo>
                    {
                        new AssociationCreateOrUpdateInfo(
                            subject: wildcardSubject,
                            associationType: "value",
                            lifecycle: "STRONG",
                            frozen: null,
                            schema: null,
                            normalize: null)
                    }));

                try
                {
                    using (var producer =
                        new ProducerBuilder<string, SubjectNameStrategyTestPoco>(producerConfig)
                            .SetValueSerializerBuilder(
                                new JsonSerializerBuilder<SubjectNameStrategyTestPoco>()
                                    .SetSchemaRegistryClient(schemaRegistry)
                                    .SetSerializerConfig(new JsonSerializerConfig
                                    {
                                        SubjectNameStrategy = SubjectNameStrategy.Associated
                                    }))
                            .Build())
                    {
                        await producer.ProduceAsync(
                            topic.Name,
                            new Message<string, SubjectNameStrategyTestPoco>
                            {
                                Key = "test1",
                                Value = new SubjectNameStrategyTestPoco { Value = "test-string" }
                            },
                            TestContext.Current.CancellationToken);
                    }

                    // The wildcard association is out of scope for this cluster, so
                    // the strategy falls back to the topic name.
                    var subjects = await schemaRegistry.GetAllSubjectsAsync();
                    Assert.Contains(topic.Name + "-value", subjects);
                }
                finally
                {
                    var associations = await schemaRegistry.GetAssociationsByResourceNameAsync(
                        topic.Name, AssociatedNameStrategy.NamespaceWildcard,
                        "topic", null, null, 0, -1);
                    if (associations.Count > 0)
                    {
                        await schemaRegistry.DeleteAssociationsAsync(
                            associations[0].ResourceId, "topic",
                            new List<string> { "value" }, true);
                    }
                }
            }
        }

        /// <summary>
        ///     Skip the calling test unless the Schema Registry under test supports
        ///     the association API. Not every Schema Registry does, and a missing
        ///     association is indistinguishable from an unsupported endpoint once
        ///     the strategy has swallowed the 404.
        /// </summary>
        private static async Task SkipUnlessAssociationsAreSupported(
            ISchemaRegistryClient schemaRegistry, string topicName, string clusterId)
        {
            try
            {
                await schemaRegistry.GetAssociationsByResourceNameAsync(
                    topicName, clusterId, "topic", null, null, 0, -1);
            }
            catch (SchemaRegistryException e) when (
                e.Status == System.Net.HttpStatusCode.NotFound ||
                e.Status == System.Net.HttpStatusCode.NotImplemented ||
                e.Status == System.Net.HttpStatusCode.MethodNotAllowed)
            {
                Assert.Skip(
                    "the Schema Registry under test does not support the association API " +
                    $"(HTTP {(int)e.Status})");
            }
        }
    }
}
