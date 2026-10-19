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

using System.Threading.Tasks;
using Confluent.Kafka;
using Xunit;

namespace Confluent.SchemaRegistry.UnitTests
{
    public class SubjectNameStrategyTests
    {
        private static SerializationContext Value(string topic) =>
            new SerializationContext(MessageComponentType.Value, topic);

        private static SerializationContext Key(string topic) =>
            new SerializationContext(MessageComponentType.Key, topic);

        [Fact]
        public async Task Topic_ProducesSubjectPerComponent()
        {
            var strategy = SubjectNameStrategy.Topic.ToAsyncDelegate();

            Assert.Equal("t-value", await strategy(Value("t"), null));
            Assert.Equal("t-key", await strategy(Key("t"), null));
        }

        [Fact]
        public async Task Record_ProducesRecordTypeOrNull()
        {
            var strategy = SubjectNameStrategy.Record.ToAsyncDelegate();

            Assert.Equal("my.Record", await strategy(Value("t"), "my.Record"));
            Assert.Null(await strategy(Value("t"), null));
        }

        [Fact]
        public async Task TopicRecord_ProducesTopicAndRecordTypeOrNull()
        {
            var strategy = SubjectNameStrategy.TopicRecord.ToAsyncDelegate();

            Assert.Equal("t-my.Record", await strategy(Value("t"), "my.Record"));
            Assert.Null(await strategy(Value("t"), null));
        }

        [Fact]
        public async Task None_ProducesNull()
        {
            var strategy = SubjectNameStrategy.None.ToAsyncDelegate();

            Assert.Null(await strategy(Value("t"), "my.Record"));
        }

        [Theory]
        [InlineData(SubjectNameStrategy.Topic)]
        [InlineData(SubjectNameStrategy.Record)]
        [InlineData(SubjectNameStrategy.TopicRecord)]
        [InlineData(SubjectNameStrategy.None)]
        public void RepeatedCalls_ReuseTheSameTask(SubjectNameStrategy kind)
        {
            var strategy = kind.ToAsyncDelegate();

            var first = strategy(Value("t"), "my.Record");
            var second = strategy(Value("t"), "my.Record");

            Assert.Same(first, second);
        }

        [Fact]
        public async Task DistinctInputs_AreNotConflated()
        {
            var strategy = SubjectNameStrategy.TopicRecord.ToAsyncDelegate();

            Assert.Equal("a-X", await strategy(Value("a"), "X"));
            Assert.Equal("b-X", await strategy(Value("b"), "X"));
            Assert.Equal("a-Y", await strategy(Value("a"), "Y"));
            Assert.Equal("a-X", await strategy(Value("a"), "X"));
        }

        [Fact]
        public async Task CacheIsBounded_AndKeepsProducingCorrectSubjects()
        {
            var strategy = SubjectNameStrategy.Topic.ToAsyncDelegate();

            for (int i = 0; i < 3000; i++)
            {
                Assert.Equal($"topic{i}-value", await strategy(Value($"topic{i}"), null));
            }
            Assert.Equal("topic0-value", await strategy(Value("topic0"), null));
        }
    }
}
