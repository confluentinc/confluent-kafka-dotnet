// ------------------------------------------------------------------------------
// <auto-generated>
//    Generated by avrogen, version 1.7.7.5
//    Changes to this file may cause incorrect behavior and will be lost if code
//    is regenerated
// </auto-generated>
// ------------------------------------------------------------------------------
namespace AvroSpecificWebApi.Entities
{
	using System;
	using System.Collections.Generic;
	using System.Text;
	using global::Avro;
	using global::Avro.Specific;
	
	public partial class User : ISpecificRecord
	{
		public static Schema _SCHEMA = Schema.Parse("{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"Confluent.Kafka.Examples.AvroSpecific" +
				"WebApi.Entities\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_num" +
				"ber\",\"type\":[\"int\",\"null\"]},{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"]}]}" +
				"");
		private string _name;
		private System.Nullable<int> _favorite_number;
		private string _favorite_color;
		public virtual Schema Schema
		{
			get
			{
				return User._SCHEMA;
			}
		}
		public string name
		{
			get
			{
				return this._name;
			}
			set
			{
				this._name = value;
			}
		}
		public System.Nullable<int> favorite_number
		{
			get
			{
				return this._favorite_number;
			}
			set
			{
				this._favorite_number = value;
			}
		}
		public string favorite_color
		{
			get
			{
				return this._favorite_color;
			}
			set
			{
				this._favorite_color = value;
			}
		}
		public virtual object Get(int fieldPos)
		{
			switch (fieldPos)
			{
			case 0: return this.name;
			case 1: return this.favorite_number;
			case 2: return this.favorite_color;
			default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Get()");
			};
		}
		public virtual void Put(int fieldPos, object fieldValue)
		{
			switch (fieldPos)
			{
			case 0: this.name = (System.String)fieldValue; break;
			case 1: this.favorite_number = (System.Nullable<int>)fieldValue; break;
			case 2: this.favorite_color = (System.String)fieldValue; break;
			default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Put()");
			};
		}
	}
}
