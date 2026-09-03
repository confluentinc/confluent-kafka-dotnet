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


namespace Confluent.SchemaRegistry
{
    /// <summary>
    ///     Defines a builder of <see cref="ISchemaRegistryClient" /> instances, for
    ///     use with the Schema Registry serde builders.
    ///
    ///     A client cannot be described by <see cref="SchemaRegistryConfig" /> alone:
    ///     an authentication header value provider and a proxy are objects rather
    ///     than configuration strings, so they cannot be carried in a
    ///     string-to-string property collection. This interface gathers everything
    ///     needed to construct a client into a single value that can be handed to a
    ///     serde builder, and reused across several of them.
    /// </summary>
    public interface ISchemaRegistryClientBuilder
    {
        /// <summary>
        ///     Build a Schema Registry client.
        ///
        ///     The caller owns the returned client and is responsible for disposing
        ///     it. A serde builder given this builder passes that ownership on to the
        ///     serde, and thence to the producer or consumer that built it.
        /// </summary>
        ISchemaRegistryClient Build();
    }
}
