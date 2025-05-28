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

// Disable obsolete warnings. ConstructValueSubjectName is still used a an internal implementation detail.
#pragma warning disable CS0618

using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;


namespace Confluent.SchemaRegistry
{
    public abstract class AsyncSerializer<T, TParsedSchema> : AsyncSerde<TParsedSchema>, IAsyncSerializer<T>
    {
        private const int DefaultInitialBufferSize = 1024;
        
        protected bool autoRegisterSchema = true;
        protected bool normalizeSchemas = false;
        protected ISchemaIdSerializer schemaIdSerializer = new PrefixSchemaIdSerializer();

        protected int initialBufferSize = DefaultInitialBufferSize;
        
        protected HashSet<string> subjectsRegistered = new HashSet<string>();
        
        protected AsyncSerializer(ISchemaRegistryClient schemaRegistryClient, SerdeConfig config, 
            RuleRegistry ruleRegistry = null) : base(schemaRegistryClient, config, ruleRegistry)
        {
        }

        public abstract Task<byte[]> SerializeAsync(T value, SerializationContext context);
    }
}
