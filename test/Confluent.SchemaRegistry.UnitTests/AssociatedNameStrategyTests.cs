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
using Xunit;

namespace Confluent.SchemaRegistry.UnitTests
{
    /// <summary>
    ///     Tests for the Kafka cluster id handling of
    ///     <see cref="AssociatedNameStrategy" />, which resolves subject names
    ///     against the cluster the client is connected to.
    ///
    ///     The resolver itself is exercised in the serdes unit tests, which have a
    ///     schema registry mock to observe lookups against.
    /// </summary>
    public class AssociatedNameStrategyTests
    {
        private static AssociatedNameStrategy Strategy(
            IEnumerable<KeyValuePair<string, string>> config = null)
            => new AssociatedNameStrategy(null, config);

        private static List<KeyValuePair<string, string>> ClusterIdConfig(string clusterId)
            => new List<KeyValuePair<string, string>>
            {
                new KeyValuePair<string, string>(
                    AssociatedNameStrategy.KafkaClusterIdConfig, clusterId)
            };

        [Fact]
        public void SetClusterIdResolver_IsRetained()
        {
            Func<string> resolver = () => "lkc-resolved";
            var strategy = Strategy();

            strategy.SetClusterIdResolver(resolver);

            Assert.Same(resolver, Resolver(strategy));
        }

        [Fact]
        public void SetClusterIdResolver_IsRetained_WhenConfigIsEmpty()
        {
            Func<string> resolver = () => "lkc-resolved";
            var strategy = Strategy(new List<KeyValuePair<string, string>>());

            strategy.SetClusterIdResolver(resolver);

            Assert.Same(resolver, Resolver(strategy));
        }

        [Fact]
        public void SetClusterIdResolver_DoesNotInvokeTheResolver()
        {
            int calls = 0;
            var strategy = Strategy();

            strategy.SetClusterIdResolver(() => { ++calls; return "lkc-resolved"; });

            Assert.Equal(0, calls);
        }

        [Fact]
        public void SetClusterIdResolver_TheLatestIsRetained()
        {
            Func<string> second = () => "lkc-second";
            var strategy = Strategy();

            strategy.SetClusterIdResolver(() => "lkc-first");
            strategy.SetClusterIdResolver(second);

            Assert.Same(second, Resolver(strategy));
        }

        [Fact]
        public void SetClusterIdResolver_DoesNotOverrideAConfiguredValue()
        {
            var strategy = Strategy(ClusterIdConfig("lkc-configured"));

            strategy.SetClusterIdResolver(() => "lkc-resolved");

            Assert.Equal("lkc-configured", ConfiguredClusterId(strategy));
            Assert.Null(Resolver(strategy));
        }

        [Fact]
        public void SetClusterIdResolver_DoesNotOverrideAValueConfiguredAsEmptyString()
        {
            // An explicitly configured empty value is still an explicit choice, and
            // must not be overwritten. This is why the strategy tracks whether the
            // id was set rather than comparing against null or the wildcard.
            var strategy = Strategy(ClusterIdConfig(""));

            strategy.SetClusterIdResolver(() => "lkc-resolved");

            Assert.Equal("", ConfiguredClusterId(strategy));
            Assert.Null(Resolver(strategy));
        }

        [Fact]
        public void ToAsyncDelegate_ExposesTheStrategyForTheAssociatedCase()
        {
            SubjectNameStrategy.Associated.ToAsyncDelegate(
                null, null, out AssociatedNameStrategy strategy);

            Assert.NotNull(strategy);
        }

        [Theory]
        [InlineData(SubjectNameStrategy.Topic)]
        [InlineData(SubjectNameStrategy.Record)]
        [InlineData(SubjectNameStrategy.TopicRecord)]
        [InlineData(SubjectNameStrategy.None)]
        public void ToAsyncDelegate_ExposesNoStrategyForOtherCases(SubjectNameStrategy strategy)
        {
            strategy.ToAsyncDelegate(null, null, out AssociatedNameStrategy associated);

            Assert.Null(associated);
        }

        // The configured cluster id and the resolver are private state; they are
        // observable only through the resource namespace used for association
        // lookups, so read them back reflectively rather than reaching for a
        // schema registry.
        private static string ConfiguredClusterId(AssociatedNameStrategy strategy)
            => (string)PrivateField("kafkaClusterId").GetValue(strategy);

        private static Func<string> Resolver(AssociatedNameStrategy strategy)
            => (Func<string>)PrivateField("clusterIdResolver").GetValue(strategy);

        private static System.Reflection.FieldInfo PrivateField(string name)
            => typeof(AssociatedNameStrategy).GetField(name,
                System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
    }
}
