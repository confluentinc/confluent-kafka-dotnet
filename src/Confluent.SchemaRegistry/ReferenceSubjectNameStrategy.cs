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
    /// <summary>
    ///     Construct the subject name under which a referenced schema
    ///     should be registered in Schema Registry.
    /// </summary>
    /// <param name="context">
    ///     The serialization context.
    /// </param>
    /// <param name="referenceName">
    ///     The name used to reference the schema.
    /// </param>
    public delegate string ReferenceSubjectNameStrategyDelegate(SerializationContext context, string referenceName);


    /// <summary>
    ///     Subject name strategy for referenced schemas.
    /// </summary>
    public enum ReferenceSubjectNameStrategy
    {
        /// <summary>
        ///     (default): Use the reference name as the subject name.
        /// </summary>
        ReferenceName
    }
    

    /// <summary>
    ///     Extension methods for the ReferenceSubjectNameStrategy type.
    /// </summary>
    public static class ReferenceSubjectNameStrategyExtensions
    {
        /// <summary>
        ///     Provide a functional implementation corresponding to the enum value.
        /// </summary>
        public static ReferenceSubjectNameStrategyDelegate ToDelegate(this ReferenceSubjectNameStrategy strategy)
            => (context, referenceName) => referenceName;
    }
}
