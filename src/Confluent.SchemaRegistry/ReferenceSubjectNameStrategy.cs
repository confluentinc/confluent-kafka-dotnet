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
using System;


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
        ///    ReferenceName (default): Use the reference name as the subject name.
        /// </summary>
        ReferenceName,
        /// <summary>
        ///    Qualified: Given a reference name, replace slashes with dots, and remove the .proto suffix to obtain the subject name. 
        /// For example, mypackage/myfile.proto becomes mypackage.myfile.
        /// </summary>
        Qualified,
        /// <summary>
        ///    Custom: Use a custom reference subject name strategy resolver to determine the subject name.
        /// </summary>
        Custom
    }

    /// <summary>
    /// Custom Reference Subject Name Strategy Interface
    /// </summary>
    public interface ICustomReferenceSubjectNameStrategy
    {
        /// <summary>
        /// Gets the subject name.
        /// </summary>
        /// <param name="context"></param>
        /// <param name="referenceName"></param>
        /// <returns></returns>
        string GetSubjectName(SerializationContext context, string referenceName);
    }

    /// <summary>
    ///     Configuration property names specific to the schema registry client.
    /// </summary>
    public static partial class PropertyNames
    {
        /// <summary>
        ///     The subject name strategy to use for registration / lookup of referenced schemas
        ///     Possible values: <see cref="Confluent.SchemaRegistry.ReferenceSubjectNameStrategy" />
        /// </summary>
        public const string ReferenceSubjectNameStrategy = "protobuf.serializer.reference.subject.name.strategy";
    }

    /// <summary>
    ///     Extension methods for the ReferenceSubjectNameStrategy type.
    /// </summary>
    public static class ReferenceSubjectNameStrategyExtensions
    {
        /// <summary>
        ///     Provide a functional implementation corresponding to the enum value.
        /// </summary>
        public static ReferenceSubjectNameStrategyDelegate ToDelegate(this ReferenceSubjectNameStrategy strategy, ICustomReferenceSubjectNameStrategy customReferenceSubjectNameStrategy = null)
        {
            return (context, referenceName) =>
            {
                switch (strategy)
                {
                    case ReferenceSubjectNameStrategy.ReferenceName:
                        {
                            return GetReferenceNameSubjectName(context, referenceName);
                        }
                    case ReferenceSubjectNameStrategy.Qualified:
                        {
                            return GetQualifiedSubjectName(context, referenceName);
                        }
                    case ReferenceSubjectNameStrategy.Custom:
                        {
                            if (customReferenceSubjectNameStrategy == null)
                            {
                                throw new ArgumentException($"Custom strategy requires a custom {nameof(ICustomReferenceSubjectNameStrategy)} implementation to be specified.");
                            }

                            return customReferenceSubjectNameStrategy.GetSubjectName(context, referenceName);
                        }
                    default:
                        {
                            throw new ArgumentException($"Unknown ${PropertyNames.ReferenceSubjectNameStrategy} value: {strategy}.");
                        }
                }
            };
        }

        /// <summary>
        /// Get Subject Name
        /// </summary>
        /// <param name="context"></param>
        /// <param name="referenceName"></param>
        /// <returns></returns>
        public static string GetQualifiedSubjectName(SerializationContext context, string referenceName)
        {
            return referenceName.Replace(".proto", string.Empty).Replace("/", ".");
        }

        /// <summary>
        /// Get Subject Name
        /// </summary>
        /// <param name="context"></param>
        /// <param name="referenceName"></param>
        /// <returns></returns>
        public static string GetReferenceNameSubjectName(SerializationContext context, string referenceName)
        {
            return referenceName;
        }
    }
}
