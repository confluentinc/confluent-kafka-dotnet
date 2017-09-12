using System;
using System.Runtime.Serialization;

namespace  Confluent.Kafka.SchemaRegistry
{
    [DataContract]
    public class Schema : IComparable<Schema>, IEquatable<Schema>
    {
        /// <summary>
        /// Subject where the schema is registred
        /// </summary>
        [DataMember(Name = "subject")]
        public string Subject { get; set; }

        /// <summary>
        /// Verion of the schema in the subject
        /// </summary>
        [DataMember(Name = "version")]
        public int Version { get; set; }

        /// <summary>
        /// Globally unique identifier of the schema
        /// </summary>
        [DataMember(Name = "id")]
        public int Id { get; set; }

        /// <summary>
        /// Schema in unindented string
        /// </summary>
        [DataMember(Name = "schema")]
        public string SchemaString { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="subject"></param>
        /// <param name="version">version du schema (liée à l'id), >= 0</param>
        /// <param name="id">id du schema (global), >= 0</param>
        /// <param name="schema"></param>
        /// <exception cref="ArgumentException">id ou version inférieur à 0</exception>
        /// <exception cref="ArgumentNullException">subject ou schema null ou vide</exception>
        public Schema(string subject, int version, int id, string schema)
        {
            if (string.IsNullOrEmpty(subject))
            {
                throw new ArgumentNullException(nameof(subject));
            }
            if (string.IsNullOrEmpty(schema))
            {
                throw new ArgumentNullException(nameof(schema));
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
            SchemaString = schema;
        }

        public override string ToString()
        {
            return $"{{subject={Subject},version={Version},id={Id},schema={SchemaString}}}";
        }

        public override bool Equals(object obj)
        {
            if (obj == null || GetType() != obj.GetType())
            {
                return false;
            }

            Schema that = (Schema)obj;
            return Equals(that);
        }
        
        public override int GetHashCode()
        {
            int result = Subject.GetHashCode();
            result = 31 * result + Version;
            result = 31 * result + Id;
            result = 31 * result + SchemaString.GetHashCode();
            return result;
        }
        
        public int CompareTo(Schema other)
        {
            int result = string.Compare(Subject, other.Subject, StringComparison.Ordinal);
            if (result == 0)
            {
                result = Version - other.Version;
            }
            return result;
        }

        public bool Equals(Schema other)
        {
            return Version == other.Version &&
                   Id == other.Id &&
                   Subject == other.Subject &&
                   SchemaString == other.SchemaString;
        }
    }
}
