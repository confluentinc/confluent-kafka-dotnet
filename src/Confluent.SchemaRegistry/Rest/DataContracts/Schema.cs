// Copyright 2016-2020 Confluent Inc.
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
using System.Collections.Generic;


namespace  Confluent.SchemaRegistry
{
    /// <summary>
    ///     Represents a schema.
    /// </summary>
    [DataContract]
    public class Schema : IComparable<Schema>, IEquatable<Schema>
    {
        #region API backwards-compatibility hack

        /// <summary>
        ///     DEPRECATED. The subject the schema is registered against.
        /// </summary>
        [Obsolete("Included to maintain API backwards compatibility only. Use RegisteredSchema instead. This property will be removed in a future version of the library.")]
        public string Subject { get; set; }

        /// <summary>
        ///     DEPRECATED. The schema version.
        /// </summary>
        [Obsolete("Included to maintain API backwards compatibility only. Use RegisteredSchema instead. This property will be removed in a future version of the library.")]
        public int Version { get; set; }

        /// <summary>
        ///     DEPRECATED. Unique identifier of the schema.
        /// </summary>
        [Obsolete("Included to maintain API backwards compatibility only. Use RegisteredSchema instead. This property will be removed in a future version of the library.")]
        public int Id { get; set; }

        /// <summary>
        ///     DEPRECATED. Initializes a new instance of the Schema class.
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
        [Obsolete("Included to maintain API backwards compatibility only. Use RegisteredSchema instead. This property will be removed in a future version of the library.")]
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
            SchemaType = SchemaType.Avro;
        }

        #endregion


        /// <summary>
        ///     A string representation of the schema.
        /// </summary>
        [DataMember(Name = "schema")]
        public string SchemaString { get; set; }

        /// <summary>
        ///     A list of schemas referenced by this schema.
        /// </summary>
        [DataMember(Name = "references")]
        public List<SchemaReference> References { get; set; }

        [DataMember(Name = "schemaType")]
        internal string SchemaType_String { get; set; }

        /// <summary>
        ///     The type of schema
        /// </summary>
        /// <remarks>
        ///     The .NET serialization framework has no way to convert
        ///     an enum to a corresponding string value, so this property
        ///     is backed by a string property, which is what is serialized.
        /// </remarks>
        public SchemaType SchemaType
        {
            get
            {
                switch (SchemaType_String)
                {
                    case "AVRO": return SchemaType.Avro;
                    case "PROTOBUF": return SchemaType.Protobuf;
                    case "JSON": return SchemaType.Json;
                }
                throw new InvalidOperationException($"Invalid program state: Unknown schema type {SchemaType_String}");
            }
            set
            {
                switch (value)
                {
                    case SchemaType.Avro: SchemaType_String = "AVRO"; break;
                    case SchemaType.Protobuf: SchemaType_String = "PROTOBUF"; break;
                    case SchemaType.Json: SchemaType_String = "JSON"; break;
                    default: throw new InvalidOperationException($"Invalid program state: Unknown schema type {SchemaType_String}");
                }
            }
        }

        /// <summary>
        ///     Empty constructor for serialization
        /// </summary>
        protected Schema() { }

        /// <summary>
        ///     Initializes a new instance of this class.
        /// </summary>
        /// <param name="schemaString">
        ///     String representation of the schema.
        /// </param>
        /// <param name="schemaType">
        ///     The schema type: AVRO, PROTOBUF, JSON
        /// </param>
        /// <param name="references">
        ///     A list of schemas referenced by this schema.
        /// </param>
        public Schema(string schemaString, List<SchemaReference> references, SchemaType schemaType)
        {
            SchemaString = schemaString;
            References = references;
            SchemaType = schemaType;
        }

        /// <summary>
        ///     Initializes a new instance of this class.
        /// </summary>
        /// <param name="schemaString">
        ///     String representation of the schema.
        /// </param>
        /// <param name="schemaType">
        ///     The schema type: AVRO, PROTOBUF, JSON
        /// </param>
        public Schema(string schemaString, SchemaType schemaType)
        {
            SchemaString = schemaString;
            References = new List<SchemaReference>();
            SchemaType = schemaType;
        }

        /// <summary>
        ///     Determines whether this instance and a specified object, which must also be an
        ///     instance of this type, have the same value (Overrides Object.Equals(Object))
        /// </summary>
        /// <param name="obj">
        ///     The instance to compare to this instance.
        /// </param>
        /// <returns>
        ///     true if obj is of the required type and its value is the same as this instance;
        ///     otherwise, false. If obj is null, the method returns false.
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
        ///     Determines whether this instance and another specified object of the same type are
        ///     the same.
        /// </summary>
        /// <param name="other">
        ///     The instance to compare to this instance.
        /// </param>
        /// <returns>
        ///     true if the value of the other parameter is the same as the value of this instance; 
        ///     otherwise, false. If other is null, the method returns false.
        /// </returns>
        public bool Equals(Schema other)
            => this.SchemaString == other.SchemaString;

        /// <summary>
        ///     Returns a hash code for this instance.
        /// </summary>
        /// <returns>
        ///     An integer that specifies a hash value for this instance.
        /// </returns>
        /// <remarks>
        ///     The hash code returned is that of the Schema property,
        ///     since the other properties are effectively derivatives
        ///     of this property.
        /// </remarks>
        public override int GetHashCode()
        {
            return SchemaString.GetHashCode();
        }

        /// <summary>
        ///     Compares this instance with another instance of this object type and indicates whether
        ///     this instance precedes, follows, or appears in the same position in the sort order
        ///     as the specified schema reference.
        /// </summary>
        /// <param name="other">
        ///     The instance to compare with this instance.
        /// </param>
        /// <returns>
        ///     A 32-bit signed integer that indicates whether this instance precedes, follows, or
        ///     appears in the same position in the sort order as the other parameter. Less than 
        ///     zero: this instance precedes other. Zero: this instance has the same position in
        ///     the sort order as other. Greater than zero: This instance follows other OR other 
        ///     is null.
        /// </returns>
        /// <remarks>
        ///     This method considers only the Schema property, since the other two properties are
        ///     effectively derivatives of this property.
        /// </remarks>
        public int CompareTo(Schema other)
        {
            return SchemaString.CompareTo(other.SchemaString);

            // If the schema strings are equal and any of the other properties are not,
            // then this is a logical error. Assume that this prevented/handled elsewhere.
        }

        /// <summary>
        ///     Returns a summary string representation of the object.
        /// </summary>
        /// <returns>
        ///     A string that represents the object.
        /// </returns>
        public override string ToString()
            => $"{{length={SchemaString.Length}, type={SchemaType}, references={References.Count}}}";


        /// <summary>
        ///     implicit cast to string.
        /// </summary>
        [Obsolete("Included to maintain API backwards compatibility only. This method will be removed in a future release.")]
        public static implicit operator string(Schema s) => s.SchemaString;
    }
}
