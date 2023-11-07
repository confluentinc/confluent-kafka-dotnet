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

using Confluent.Kafka;


namespace Confluent.SchemaRegistry
{
    ///<summary>
    /// This interface is used by the serializer to determine the subject name under which
    /// the referenced schema should be registered in the schema registry.
    /// </summary>
    public interface IReferenceSubjectNameStrategy
    {
        /// <summary>
        /// For a given reference name and serialization context, returns the subject name under which the
        /// referenced schema should be registered in the schema registry.
        /// </summary>
        /// <param name="context">Context relevant to the serialization operation.</param>
        /// <param name="referenceName">The name of the reference.</param>
        /// <returns></returns>
        string GetSubjectName(SerializationContext context, string referenceName);
    }

    /// <summary>
    /// The default strategy used by serializer that uses the reference name as the subject name
    /// under which the referenced schema should be registered in schema registry.
    /// </summary>
    public class DefaultReferenceSubjectNameStrategy : IReferenceSubjectNameStrategy
    {
        /// <summary>
        /// <inheritdoc />
        /// </summary>
        public string GetSubjectName(SerializationContext context, string referenceName)
        {
            return referenceName;
        }
    }
}
