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


namespace Confluent.Kafka
{
    /// <summary>
    ///     Context relevant to a serialization or deserialization operation.
    /// </summary>
    public struct SerializationContext
    {
        /// <summary>
        ///     The default SerializationContext value (representing no context defined).
        /// </summary>
        public static SerializationContext Empty
            => default(SerializationContext);

        /// <summary>
        ///     Create a new SerializationContext object instance.
        /// </summary>
        /// <param name="component">
        ///     The component of the message the serialization operation relates to.
        /// </param>
        /// <param name="topic">
        ///     The topic the data is being written to or read from.
        /// </param>
        /// <param name="headers">
        ///     The collection of message headers (or null). Specifying null or an
        ///     empty list are equivalent. The order of headers is maintained, and
        ///     duplicate header keys are allowed.
        /// </param>       
        public SerializationContext(MessageComponentType component, string topic, Headers headers = null)
        {
            Component = component;
            Topic = topic;
            Headers = headers;
        }

        /// <summary>
        ///     The topic the data is being written to or read from.
        /// </summary>
        public string Topic { get; private set; }
        
        /// <summary>
        ///     The component of the message the serialization operation relates to.
        /// </summary>
        public MessageComponentType Component { get; private set; }

        /// <summary>
        ///     The collection of message headers (or null). Specifying null or an
        ///     empty list are equivalent. The order of headers is maintained, and
        ///     duplicate header keys are allowed.
        /// </summary>
        public Headers Headers { get; private set; }
    }
}
