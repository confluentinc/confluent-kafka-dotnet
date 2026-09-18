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


namespace Confluent.Kafka
{
    /// <summary>
    ///     Implemented by serializers and deserializers that can make use of the id
    ///     of the Kafka cluster the client is connected to.
    ///
    ///     A producer or consumer does not resolve the cluster id itself. During
    ///     construction it hands each serializer or deserializer a resolver, via
    ///     <see cref="SetClusterIdResolver" />, which the serde invokes whenever it
    ///     actually needs the id. Resolving the id requires the client to
    ///     have reached a broker, which is not always possible during construction -
    ///     an OAUTHBEARER token refresh callback, for instance, is only served from
    ///     the poll loop - so deferring it keeps construction from blocking.
    ///
    ///     This interface is deliberately separate from <see cref="ISerializer{T}" />
    ///     and friends: those are implemented by application code, so adding members
    ///     to them would be a breaking change. Use the <c>SetClusterIdResolver</c>
    ///     extension methods to hand a resolver to an arbitrary serializer or
    ///     deserializer.
    /// </summary>
    public interface IClusterIdAware
    {
        /// <summary>
        ///     Supply a resolver for the id of the Kafka cluster the client is
        ///     connected to.
        ///
        ///     The resolver may block while the client reaches a broker, and returns
        ///     null if it cannot do so within the client's timeout. Implementations
        ///     are expected to invoke it lazily, only when the id is actually needed.
        ///
        ///     The resolver is bound to the client that supplied it, and throws
        ///     <see cref="ObjectDisposedException" /> once that client has been
        ///     disposed. A serializer or deserializer handed to a producer or
        ///     consumer must therefore not be used after that client is disposed,
        ///     unless the cluster id it needs was specified via configuration.
        ///
        ///     Implementations must ignore the resolver when the cluster id is not
        ///     relevant to their configuration, or when it was specified explicitly
        ///     via configuration, so that a configured cluster id is never
        ///     overwritten.
        /// </summary>
        /// <param name="clusterIdResolver">
        ///     Resolves the Kafka cluster id.
        /// </param>
        void SetClusterIdResolver(Func<string> clusterIdResolver);
    }
}
