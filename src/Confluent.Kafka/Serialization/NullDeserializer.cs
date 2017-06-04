// Copyright 2016-2017 Confluent Inc.
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


namespace Confluent.Kafka.Serialization
{
    /// <summary>
    ///     A dummy deserializer for use with values that must be null.
    /// </summary>
    public class NullDeserializer : IDeserializer<Null>
    {
        /// <summary>
        ///     'Deserializes' a null value to a null value.
        /// </summary>
        /// <param name="data">
        ///     The data to deserialize (must be null).
        /// </param>        
        /// <param name="topic">
        ///     The topic associated with the data (ignored by this deserializer).
        /// </param>
        /// <returns>
        ///     null
        /// </returns>
        public Null Deserialize(string topic, byte[] data)
        {
            if (data != null)
            {
                throw new System.ArgumentException("NullDeserializer may only be used to deserialize data that is null.");
            }

            return null;
        }

        /// <include file='../include_docs.xml' path='API/Member[@name="IDeserializer_Configure"]/*' />
        public IEnumerable<KeyValuePair<string, object>> Configure(IEnumerable<KeyValuePair<string, object>> config, bool isKey)
            => config;
    }
}
