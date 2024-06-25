// Copyright 2020 Confluent Inc.
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


namespace Confluent.SchemaRegistry
{
    public abstract class AsyncDeserializer<T, TParsedSchema> : AsyncSerde<TParsedSchema>, IAsyncDeserializer<T>
    {
        protected readonly int headerSize =  sizeof(int) + sizeof(byte);
        
        protected AsyncDeserializer(ISchemaRegistryClient schemaRegistryClient, SerdeConfig config, 
            IList<IRuleExecutor> ruleExecutors = null) : base(schemaRegistryClient, config, ruleExecutors)
        {
        }
        public abstract Task<T> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, SerializationContext context);
    }
}
