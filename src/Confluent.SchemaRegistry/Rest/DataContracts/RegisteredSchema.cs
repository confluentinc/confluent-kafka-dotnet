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

// disable warning for overriding obsolete methods.
#pragma warning disable CS0672
#pragma warning disable CS0618

using System;
using System.Collections.Generic;
using System.Runtime.Serialization;


namespace Confluent.SchemaRegistry
{
    /// <summary>
    ///     Represents a Schema stored in Schema Registry.
    /// </summary>
    /// <remarks>
    ///     Inherits from Schema to enable API backwards compatibility only.
    ///     In the future, this relationship will be removed.
    /// </remarks>
    [DataContract]
    public class RegisteredSchema : Schema, IComparable<RegisteredSchema>, IEquatable<RegisteredSchema>
    {
        private int? hashCode;

        /// <summary>
        ///     The subject the schema is registered against.
        /// </summary>
        [DataMember(Name = "subject")]
        public new string Subject { get; set; }

        /// <summary>
        ///     The schema version.
        /// </summary>
        [DataMember(Name = "version")]
        public new int Version { get; set; }

        /// <summary>
        ///     Unique identifier of the schema.
        /// </summary>
        [DataMember(Name = "id")]
        public new int Id { get; set; }

        /// <summary>
        ///     The unregistered schema corresponding to this schema.
        /// </summary>
        public Schema Schema
        {
            get
            {
                return new Schema(SchemaString, References, SchemaType);
            }
        }

        /// <summary>
        ///     Included to enable API backwards compatibility only, do not use.
        /// </summary>
        [Obsolete("Included to enable API backwards compatibility. This will be removed in a future release.")]
        protected RegisteredSchema() {}

        /// <summary>
        ///     Initializes a new instance of this class.
        /// </summary>
        /// <param name="subject">
        ///     The subject the schema is registered against.
        /// </param>
        /// <param name="version">
        ///     The schema version, >= 0
        /// </param>
        /// <param name="id">
        ///     The globally unique identifier of the schema, >= 0
        /// </param>
        /// <param name="schemaString">
        ///     String representation of the schema.
        /// </param>
        /// <param name="schemaType">
        ///     The schema type: AVRO, PROTOBUF, JSON
        /// </param>
        /// <param name="references">
        ///     A list of schemas referenced by this schema.
        /// </param>
        public RegisteredSchema(string subject, int version, int id, string schemaString, SchemaType schemaType, List<SchemaReference> references)
        {
            if (string.IsNullOrEmpty(schemaString))
            {
                throw new ArgumentNullException(nameof(schemaString));
            }
            if (version < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(version));
            }
            if (id < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(id));
            }

            Subject = subject;
            Version = version;
            Id = id;
            SchemaString = schemaString;
            References = references;
            SchemaType = schemaType;
        }

        /// <summary>
        ///     Returns a summary string representation of the object.
        /// </summary>
        /// <returns>
        ///     A string that represents the object.
        /// </returns>
        public override string ToString()
            => $"{{length={SchemaString.Length}, subject={Subject}, version={Version}, id={Id}, type={SchemaType}, dependencies={References.Count}}}";

        /// <summary>
        ///     Returns a hash code for this Schema.
        /// </summary>
        /// <returns>
        ///     An integer that specifies a hash value for this Schema.
        /// </returns>
        public override int GetHashCode()
        {
            if (this.hashCode == null)
            {
                int h = Subject.GetHashCode();
                h = 31 * h + Version;
                h = 31 * h + Id;
                h = 31 * h + SchemaString.GetHashCode();
                this.hashCode = h;
            }
            return this.hashCode.Value;
        }

        /// <summary>
        ///     Compares this instance with a specified RegisteredSchema object and indicates whether this 
        ///     instance precedes, follows, or appears in the same position in the sort order as
        ///     the specified schema.
        /// </summary>
        /// <param name="other">
        ///     The schema to compare with this instance.
        /// </param>
        /// <returns>
        ///     A 32-bit signed integer that indicates whether this instance precedes, follows, or
        ///     appears in the same position in the sort order as the other parameter. Less than 
        ///     zero: this instance precedes other. Zero: this instance has the same position in
        ///     the sort order as other. Greater than zero: This instance follows other OR other 
        ///     is null.
        /// </returns>
        public int CompareTo(RegisteredSchema other)
        {
            int result = string.Compare(Subject, other.Subject, StringComparison.Ordinal);
            if (result == 0)
            {
                return Version.CompareTo(other.Version);
            }

            return result;

            // If the schema strings + version are equal and any of the other properties are not,
            // then this is a logical error. Assume that this prevented/handled elsewhere.   
        }

        /// <summary>
        ///     Determines whether this instance and a specified object, which must also be a Schema 
        ///     object, have the same value (Overrides Object.Equals(Object))
        /// </summary>
        /// <param name="obj">
        ///     The Schema to compare to this instance.
        /// </param>
        /// <returns>
        ///     true if obj is a Schema and its value is the same as this instance; otherwise, false. 
        ///     If obj is null, the method returns false.
        /// </returns>
        public override bool Equals(object obj)
        {
            if (obj == null || GetType() != obj.GetType())
            {
                return false;
            }

            RegisteredSchema that = (RegisteredSchema)obj;
            return Equals(that);
        }

        /// <summary>
        ///     Determines whether this instance and another specified Schema object are the same.
        /// </summary>
        /// <param name="other">
        ///     The schema to compare to this instance.
        /// </param>
        /// <returns>
        ///     true if the value of the other parameter is the same as the value of this instance; 
        ///     otherwise, false. If other is null, the method returns false.
        /// </returns>
        public bool Equals(RegisteredSchema other)
            => Version == other.Version &&
               Id == other.Id &&
               Subject == other.Subject &&
               SchemaString == other.SchemaString;
    }
}
