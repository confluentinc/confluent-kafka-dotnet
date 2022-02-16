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

using System.Buffers;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;

namespace Confluent.Kafka.IntegrationTests
{
    class SimpleAsyncSerializer : IAsyncSerializer<string>
    {
        public async Task SerializeAsync(string data, SerializationContext context, IBufferWriter<byte> bufferWriter)
        {
            await Task.Delay(500).ConfigureAwait(false);
            Serializers.Utf8.Serialize(data, context, bufferWriter);
        }

        public ISerializer<string> SyncOverAsync()
        {
            return new SyncOverAsyncSerializer<string>(this);
        }
    }

    class SimpleSyncSerializer : ISerializer<string>
    {
        public void Serialize(string data, SerializationContext context, IBufferWriter<byte> bufferWriter)
        {
            Thread.Sleep(500);
            Serializers.Utf8.Serialize(data, context, bufferWriter);
        }
    }
}
