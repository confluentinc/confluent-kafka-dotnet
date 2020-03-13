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

using System;
using System.Runtime.Serialization;


namespace Confluent.SchemaRegistry
{
    /// <summary>
    ///     Represents a reference to a Schema stored in Schema Registry.
    /// </summary>
    [DataContract]
    public class SchemaReference : IComparable<SchemaReference>, IEquatable<SchemaReference>
    {
        /// <summary>
        ///     The schema name.
        /// </summary>
        [DataMember(Name = "name")]
        public string Name { get; set; }

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

        private SchemaReference() {}

        /// <summary>
        ///     Initializes a new instance of the SchemaReference class.
        /// </summary>
        /// <param name="name">
        ///     The name the schema is registered against.
        /// </param>
        /// <param name="subject">
        ///     The subject the schema is registered against.
        /// </param>
        /// <param name="version">
        ///     The schema version, >= 0
        /// </param>
        public SchemaReference(string name, string subject, int version)
        {
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentNullException(nameof(name));
            }
            if (string.IsNullOrEmpty(subject))
            {
                throw new ArgumentNullException(nameof(subject));
            }
            if (version < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(version));
            }

            Name = name;
            Subject = subject;
            Version = version;
        }

        /// <summary>
        ///     Returns a string representation of the Schema object.
        /// </summary>
        /// <returns>
        ///     A string that represents the schema object.
        /// </returns>
        public override string ToString()
            => $"{{name={Name}, subject={Subject}, version={Version}}}";

        /// <summary>
        ///     Returns a hash code for this SchemaReference.
        /// </summary>
        /// <returns>
        ///     An integer that specifies a hash value for this SchemaReference.
        /// </returns>
        public override int GetHashCode()
        {
            int result = Name.GetHashCode();
            result = 31 * result + Subject.GetHashCode();
            result = 31 * result + Version;
            return result;
        }

        /// <summary>
        ///     Compares this instance with a specified SchemaReference object and indicates whether
        ///     this  instance precedes, follows, or appears in the same position in the sort order
        ///     as the specified schema reference.
        /// </summary>
        /// <param name="other">
        ///     The schema reference to compare with this instance.
        /// </param>
        /// <returns>
        ///     A 32-bit signed integer that indicates whether this instance precedes, follows, or
        ///     appears in the same position in the sort order as the other parameter. Less than 
        ///     zero: this instance precedes other. Zero: this instance has the same position in
        ///     the sort order as other. Greater than zero: This instance follows other OR other 
        ///     is null.
        /// </returns>
        public int CompareTo(SchemaReference other)
        {
            int result = string.Compare(Name, other.Name, StringComparison.Ordinal);
            if (result != 0)
            {
                return result;
            }

            result = string.Compare(Subject, other.Subject, StringComparison.Ordinal);
            if (result != 0)
            {
                return result;
            }

            return Version.CompareTo(other.Version);
        }

        /// <summary>
        ///     Determines whether this instance and a specified object, which must also be a
        ///     SchemaReference object, have the same value (Overrides Object.Equals(Object))
        /// </summary>
        /// <param name="obj">
        ///     The SchemaReference to compare to this instance.
        /// </param>
        /// <returns>
        ///     true if obj is a SchemaReference and its value is the same as this instance;
        ///     otherwise, false. If obj is null, the method returns false.
        /// </returns>
        public override bool Equals(object obj)
        {
            if (obj == null || GetType() != obj.GetType())
            {
                return false;
            }

            SchemaReference that = (SchemaReference)obj;
            return Equals(that);
        }

        /// <summary>
        ///     Determines whether this instance and another specified SchemaReference object are
        ///     the same.
        /// </summary>
        /// <param name="other">
        ///     The schema reference to compare to this instance.
        /// </param>
        /// <returns>
        ///     true if the value of the other parameter is the same as the value of this instance; 
        ///     otherwise, false. If other is null, the method returns false.
        /// </returns>
        public bool Equals(SchemaReference other)
            => Version == other.Version &&
               Subject == other.Subject &&
               Name == other.Name;
    }
}
