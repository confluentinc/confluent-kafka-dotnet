using Avro;
using Avro.Specific;

namespace Confluent.Kafka.Examples.AvroSpecific
{
    // This class is modified from the auto-generated class
    public partial class User2 : ISpecificRecord
    {
        // The namespace and name of the schema does not need to match the namespace and name of the actual .NET data class.
        // Think about a use case where a producer and a consumer use completely different languages.
        public static Schema _SCHEMA = Schema.Parse("{\"type\":\"record\",\"name\":\"SomeUser\",\"namespace\":\"SomeNamespace" +
                "\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":[\"i" +
                "nt\",\"null\"]},{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"]}]}");

        private string _name;
        private int? _favorite_number;
        private string _favorite_color;

        public virtual Schema Schema
        {
            get
            {
                return _SCHEMA;
            }
        }

        public string name
        {
            get
            {
                return _name;
            }
            set
            {
                _name = value;
            }
        }
        public int? favorite_number
        {
            get
            {
                return _favorite_number;
            }
            set
            {
                _favorite_number = value;
            }
        }
        public string favorite_color
        {
            get
            {
                return _favorite_color;
            }
            set
            {
                _favorite_color = value;
            }
        }
        public virtual object Get(int fieldPos)
        {
            switch (fieldPos)
            {
                case 0: return name;
                case 1: return favorite_number;
                case 2: return favorite_color;
                default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Get()");
            };
        }
        public virtual void Put(int fieldPos, object fieldValue)
        {
            switch (fieldPos)
            {
                case 0: name = (System.String)fieldValue; break;
                case 1: favorite_number = (System.Nullable<int>)fieldValue; break;
                case 2: favorite_color = (System.String)fieldValue; break;
                default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Put()");
            };
        }
    }
}
