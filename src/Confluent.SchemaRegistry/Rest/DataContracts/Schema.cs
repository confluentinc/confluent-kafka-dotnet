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
using System.Runtime.Serialization;


namespace Confluent.SchemaRegistry
{
    /// <summary>
    ///     Represents a Schema stored in Schema Registry.
    /// </summary>
    [DataContract]
    public class Schema : IComparable<Schema>, IEquatable<Schema>
    {
        /// <summary>
        ///     The subject the schema is registered against.
        /// </summary>
        [DataMember(Name = "subject")]
        public string Subject { get; set; }

        /// <summary>
        ///     The schema version.
        /// </summary>
        [DataMember(Name = "version")]
        public int Version { get; set; }

        /// <summary>
        ///     Unique identifier of the schema.
        /// </summary>
        [DataMember(Name = "id")]
        public int Id { get; set; }

        /// <summary>
        ///     A string representation of the schema.
        /// </summary>
        [DataMember(Name = "schema")]
        public string SchemaString { get; set; }

        private Schema() {}

        /// <summary>
        ///     Initializes a new instance of the Schema class.
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
        public Schema(string subject, int version, int id, string schemaString)
        {
            if (string.IsNullOrEmpty(subject))
            {
                throw new ArgumentNullException(nameof(subject));
            }
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
        }

        /// <summary>
        ///     Returns a string representation of the Schema object.
        /// </summary>
        /// <returns>
        ///     A string that represents the schema object.
        /// </returns>
        public override string ToString()
            => $"{{subject={Subject}, version={Version}, id={Id}}}";
        
        /// <summary>
        ///     Returns a hash code for this Schema.
        /// </summary>
        /// <returns>
        ///     An integer that specifies a hash value for this Schema.
        /// </returns>
        public override int GetHashCode()
        {
            int result = Subject.GetHashCode();
            result = 31 * result + Version;
            result = 31 * result + Id;
            result = 31 * result + SchemaString.GetHashCode();
            return result;
        }
        
        /// <summary>
        ///     Compares this instance with a specified Schema object and indicates whether this 
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
        public int CompareTo(Schema other)
        {
            if (other == null)
            {
                throw new ArgumentException("cannot compare object of type Schema with null.");
            }

            int result = string.Compare(Subject, other.Subject, StringComparison.Ordinal);
            if (result == 0)
            {
                return Version.CompareTo(other.Version);
            }

            return result;
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

            Schema that = (Schema)obj;
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
        public bool Equals(Schema other)
            => Version == other.Version &&
               Id == other.Id &&
               Subject == other.Subject &&
               SchemaString == other.SchemaString;
    }
}
