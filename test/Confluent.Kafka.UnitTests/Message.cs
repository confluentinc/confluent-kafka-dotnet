// Copyright 2019 Confluent Inc.
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

using Xunit;


namespace Confluent.Kafka.UnitTests
{
    public class Message
    {
        [Fact]
        public void CopyConstructor()
        {
            var testTimestamp = new Timestamp(1003, TimestampType.LogAppendTime);
            var testHeaders = new Headers { new Header("myheader", new byte[] { 42 }) };
            var testKey = new byte[] { 4 };
            var testValue = new byte[] { 7, 5, 2 };

            var m = new Message<byte[], byte[]>();
            m.Headers = testHeaders;
            m.Timestamp = testTimestamp;
            m.Key = testKey;
            m.Value = testValue;

            var copy = new Confluent.Kafka.Message(m);
            Assert.Equal(testValue, copy.Value);
            Assert.Equal(testKey, copy.Key);
            Assert.Equal(testTimestamp, copy.Timestamp);
            Assert.Equal(testHeaders, copy.Headers);
        }
    }
}
