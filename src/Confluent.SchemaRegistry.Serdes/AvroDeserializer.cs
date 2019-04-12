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

using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;


namespace Confluent.SchemaRegistry.Serdes
{
    public class AvroDeserializer<T> : IDeserializer<T>
    {
        AsyncAvroDeserializer<T> asyncDeserializer;

        public AvroDeserializer(ISchemaRegistryClient schemaRegistryClient, IEnumerable<KeyValuePair<string, string>> config = null)
        {
            asyncDeserializer = new AsyncAvroDeserializer<T>(schemaRegistryClient, config);
        }

        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
            => asyncDeserializer.DeserializeAsync(new ReadOnlyMemory<byte>(data.ToArray()), isNull, context)
                .ConfigureAwait(continueOnCapturedContext: false)
                .GetAwaiter()
                .GetResult();
    }
}
