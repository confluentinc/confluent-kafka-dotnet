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

using Confluent.Kafka.SyncOverAsync;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Extension methods that interrogate a serializer or deserializer for
    ///     optional capabilities - <see cref="IClusterIdAware" /> and
    ///     <see cref="ISerdeOwnedResources" /> - without requiring it to implement
    ///     them.
    ///
    ///     Every method here is safe to call on any serializer or deserializer,
    ///     including the built-in ones and application-supplied delegates: an
    ///     instance that does not implement the relevant interface reports no
    ///     capability and is left untouched.
    ///
    ///     An instance wrapped with <c>AsSyncOverAsync()</c> is unwrapped first, so
    ///     the capabilities of the underlying async serializer or deserializer are
    ///     the ones that count.
    /// </summary>
    public static class SerdeExtensions
    {
        /// <summary>
        ///     Whether <paramref name="serializer" /> still requires the id of the
        ///     Kafka cluster the client is connected to.
        /// </summary>
        public static bool NeedsClusterId<T>(this ISerializer<T> serializer)
            => NeedsClusterId(Unwrap(serializer));

        /// <summary>
        ///     Whether <paramref name="serializer" /> still requires the id of the
        ///     Kafka cluster the client is connected to.
        /// </summary>
        public static bool NeedsClusterId<T>(this IAsyncSerializer<T> serializer)
            => NeedsClusterId((object)serializer);

        /// <summary>
        ///     Whether <paramref name="deserializer" /> still requires the id of the
        ///     Kafka cluster the client is connected to.
        /// </summary>
        public static bool NeedsClusterId<T>(this IDeserializer<T> deserializer)
            => NeedsClusterId(Unwrap(deserializer));

        /// <summary>
        ///     Whether <paramref name="deserializer" /> still requires the id of the
        ///     Kafka cluster the client is connected to.
        /// </summary>
        public static bool NeedsClusterId<T>(this IAsyncDeserializer<T> deserializer)
            => NeedsClusterId((object)deserializer);

        /// <summary>
        ///     Supply the id of the Kafka cluster the client is connected to, if
        ///     <paramref name="serializer" /> makes use of it.
        /// </summary>
        public static void SetClusterId<T>(this ISerializer<T> serializer, string clusterId)
            => SetClusterId(Unwrap(serializer), clusterId);

        /// <summary>
        ///     Supply the id of the Kafka cluster the client is connected to, if
        ///     <paramref name="serializer" /> makes use of it.
        /// </summary>
        public static void SetClusterId<T>(this IAsyncSerializer<T> serializer, string clusterId)
            => SetClusterId((object)serializer, clusterId);

        /// <summary>
        ///     Supply the id of the Kafka cluster the client is connected to, if
        ///     <paramref name="deserializer" /> makes use of it.
        /// </summary>
        public static void SetClusterId<T>(this IDeserializer<T> deserializer, string clusterId)
            => SetClusterId(Unwrap(deserializer), clusterId);

        /// <summary>
        ///     Supply the id of the Kafka cluster the client is connected to, if
        ///     <paramref name="deserializer" /> makes use of it.
        /// </summary>
        public static void SetClusterId<T>(this IAsyncDeserializer<T> deserializer, string clusterId)
            => SetClusterId((object)deserializer, clusterId);

        /// <summary>
        ///     Release any resources <paramref name="serializer" /> created itself.
        ///     Resources supplied by the application are not released.
        /// </summary>
        public static void Dispose<T>(this ISerializer<T> serializer)
            => DisposeOwnedResources(Unwrap(serializer));

        /// <summary>
        ///     Release any resources <paramref name="serializer" /> created itself.
        ///     Resources supplied by the application are not released.
        /// </summary>
        public static void Dispose<T>(this IAsyncSerializer<T> serializer)
            => DisposeOwnedResources((object)serializer);

        /// <summary>
        ///     Release any resources <paramref name="deserializer" /> created itself.
        ///     Resources supplied by the application are not released.
        /// </summary>
        public static void Dispose<T>(this IDeserializer<T> deserializer)
            => DisposeOwnedResources(Unwrap(deserializer));

        /// <summary>
        ///     Release any resources <paramref name="deserializer" /> created itself.
        ///     Resources supplied by the application are not released.
        /// </summary>
        public static void Dispose<T>(this IAsyncDeserializer<T> deserializer)
            => DisposeOwnedResources((object)deserializer);

        private static bool NeedsClusterId(object serde)
            => serde is IClusterIdAware clusterIdAware && clusterIdAware.NeedsClusterId;

        private static void SetClusterId(object serde, string clusterId)
        {
            if (serde is IClusterIdAware clusterIdAware)
            {
                clusterIdAware.SetClusterId(clusterId);
            }
        }

        private static void DisposeOwnedResources(object serde)
        {
            if (serde is ISerdeOwnedResources owner)
            {
                owner.DisposeOwnedResources();
            }
        }

        // A serializer wrapped for use where a sync serializer is required delegates
        // to the async one, which is where any capabilities actually live.
        private static object Unwrap<T>(ISerializer<T> serializer)
            => serializer is SyncOverAsyncSerializer<T> syncOverAsync
                ? (object)syncOverAsync.AsyncSerializer
                : serializer;

        private static object Unwrap<T>(IDeserializer<T> deserializer)
            => deserializer is SyncOverAsyncDeserializer<T> syncOverAsync
                ? (object)syncOverAsync.AsyncDeserializer
                : deserializer;
    }
}
