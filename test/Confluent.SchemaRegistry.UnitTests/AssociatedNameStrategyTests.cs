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

using System.Collections.Generic;
using Xunit;

namespace Confluent.SchemaRegistry.UnitTests
{
    /// <summary>
    ///     Tests for the Kafka cluster id handling of
    ///     <see cref="AssociatedNameStrategy" />, which resolves subject names
    ///     against the cluster the client is connected to.
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
        public void NeedsClusterId_WhenNotConfigured()
        {
            Assert.True(Strategy().NeedsClusterId);
        }

        [Fact]
        public void NeedsClusterId_WhenConfigIsEmpty()
        {
            Assert.True(Strategy(new List<KeyValuePair<string, string>>()).NeedsClusterId);
        }

        [Fact]
        public void DoesNotNeedClusterId_WhenConfigured()
        {
            Assert.False(Strategy(ClusterIdConfig("lkc-configured")).NeedsClusterId);
        }

        [Fact]
        public void DoesNotNeedClusterId_WhenConfiguredAsEmptyString()
        {
            // An explicitly configured empty value is still an explicit choice, and
            // must not be overwritten. This is why the strategy tracks whether the
            // id was set rather than comparing against null or the wildcard.
            Assert.False(Strategy(ClusterIdConfig("")).NeedsClusterId);
        }

        [Fact]
        public void SetClusterId_SatisfiesTheNeed()
        {
            var strategy = Strategy();
            Assert.True(strategy.NeedsClusterId);

            strategy.SetClusterId("lkc-resolved");

            Assert.False(strategy.NeedsClusterId);
        }

        [Fact]
        public void SetClusterId_IsStickyAcrossRepeatedCalls()
        {
            var strategy = Strategy();

            strategy.SetClusterId("lkc-first");
            strategy.SetClusterId("lkc-second");

            Assert.False(strategy.NeedsClusterId);
            Assert.Equal("lkc-first", ConfiguredClusterId(strategy));
        }

        [Fact]
        public void SetClusterId_DoesNotOverrideAConfiguredValue()
        {
            var strategy = Strategy(ClusterIdConfig("lkc-configured"));

            strategy.SetClusterId("lkc-resolved");

            Assert.Equal("lkc-configured", ConfiguredClusterId(strategy));
        }

        [Fact]
        public void NeedsClusterIdFor_OnlyTheAssociatedStrategy()
        {
            Assert.True(AssociatedNameStrategy.NeedsClusterIdFor(
                SubjectNameStrategy.Associated, null));

            Assert.False(AssociatedNameStrategy.NeedsClusterIdFor(
                SubjectNameStrategy.Topic, null));
            Assert.False(AssociatedNameStrategy.NeedsClusterIdFor(
                SubjectNameStrategy.Record, null));
            Assert.False(AssociatedNameStrategy.NeedsClusterIdFor(
                SubjectNameStrategy.TopicRecord, null));
            Assert.False(AssociatedNameStrategy.NeedsClusterIdFor(
                SubjectNameStrategy.None, null));
        }

        [Fact]
        public void NeedsClusterIdFor_FalseWhenConfigured()
        {
            Assert.False(AssociatedNameStrategy.NeedsClusterIdFor(
                SubjectNameStrategy.Associated, ClusterIdConfig("lkc-configured")));
        }

        [Fact]
        public void NeedsClusterIdFor_TrueWhenSomeOtherPropertyIsConfigured()
        {
            var config = new List<KeyValuePair<string, string>>
            {
                new KeyValuePair<string, string>(
                    AssociatedNameStrategy.FallbackTypeConfig, "RECORD")
            };

            Assert.True(AssociatedNameStrategy.NeedsClusterIdFor(
                SubjectNameStrategy.Associated, config));
        }

        [Fact]
        public void ToAsyncDelegate_ExposesTheStrategyForTheAssociatedCase()
        {
            SubjectNameStrategy.Associated.ToAsyncDelegate(
                null, null, out AssociatedNameStrategy strategy);

            Assert.NotNull(strategy);
            Assert.True(strategy.NeedsClusterId);
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

        // The configured cluster id is private state; it is observable only through
        // the resource namespace used for association lookups, so read it back
        // reflectively rather than reaching for a schema registry.
        private static string ConfiguredClusterId(AssociatedNameStrategy strategy)
            => (string)typeof(AssociatedNameStrategy)
                .GetField("kafkaClusterId",
                    System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)
                .GetValue(strategy);
    }
}
