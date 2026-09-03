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
    ///     Implemented by serializers and deserializers that hold disposable
    ///     resources they created themselves, and are therefore responsible for
    ///     releasing.
    ///
    ///     A serializer or deserializer constructed by a producer or consumer from
    ///     an <see cref="ISerializerBuilder{T}" /> (or one of its siblings) is owned
    ///     by that client, and is released when the client is disposed. One that was
    ///     constructed by the application and supplied directly remains owned by the
    ///     application and is never released by the client.
    ///
    ///     This interface intentionally does not extend
    ///     <see cref="System.IDisposable" />: doing so would cause analyzers to
    ///     require disposal at every construction site in application code, which is
    ///     misleading given that a serializer holding no resources of its own has
    ///     nothing to release. Use the <c>Dispose</c> extension methods to release an
    ///     arbitrary serializer or deserializer.
    /// </summary>
    public interface ISerdeOwnedResources
    {
        /// <summary>
        ///     Release the resources this instance created itself.
        ///
        ///     Resources supplied by the application are not released.
        ///     Implementations must tolerate being called more than once.
        /// </summary>
        void DisposeOwnedResources();
    }
}
