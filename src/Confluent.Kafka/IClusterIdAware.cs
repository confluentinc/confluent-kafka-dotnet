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


namespace Confluent.Kafka
{
    /// <summary>
    ///     Implemented by serializers and deserializers that can make use of the id
    ///     of the Kafka cluster the client is connected to.
    ///
    ///     A producer or consumer resolves the cluster id once during construction,
    ///     but only if at least one of its serializers or deserializers reports
    ///     <see cref="NeedsClusterId" />, and then supplies it via
    ///     <see cref="SetClusterId" />.
    ///
    ///     This interface is deliberately separate from <see cref="ISerializer{T}" />
    ///     and friends: those are implemented by application code, so adding members
    ///     to them would be a breaking change. Use the <c>NeedsClusterId</c> and
    ///     <c>SetClusterId</c> extension methods to interrogate an arbitrary
    ///     serializer or deserializer.
    /// </summary>
    public interface IClusterIdAware
    {
        /// <summary>
        ///     Whether this instance still requires the Kafka cluster id.
        ///
        ///     Returns false when the cluster id is not relevant to this instance's
        ///     configuration, or when it has already been supplied - either
        ///     explicitly via configuration, or by an earlier call to
        ///     <see cref="SetClusterId" />.
        /// </summary>
        bool NeedsClusterId { get; }

        /// <summary>
        ///     Supply the id of the Kafka cluster the client is connected to.
        ///
        ///     Implementations must ignore the value when
        ///     <see cref="NeedsClusterId" /> is false, so that a cluster id
        ///     specified via configuration is never overwritten.
        /// </summary>
        /// <param name="clusterId">
        ///     The Kafka cluster id.
        /// </param>
        void SetClusterId(string clusterId);
    }
}
