// ------------------------------------------------------------------------------
// <auto-generated>
//    Generated by avrogen, version 1.11.0.0
//    Changes to this file may cause incorrect behavior and will be lost if code
//    is regenerated
// </auto-generated>
// ------------------------------------------------------------------------------
namespace Confluent.Kafka.Examples.AvroSpecific
{
	using System;
	using System.Collections.Generic;
	using System.Text;
	using Avro;
	using Avro.Specific;
	
	public partial class User : ISpecificRecord
	{
		public static Schema _SCHEMA = Avro.Schema.Parse("{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"confluent.io.examples.serialization.a" +
				"vro\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":" +
				"\"long\"},{\"name\":\"favorite_color\",\"type\":\"string\"}]}");
		private string _name;
		private long _favorite_number;
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
		public long favorite_number
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
			case 1: this.favorite_number = (System.Int64)fieldValue; break;
			case 2: this.favorite_color = (System.String)fieldValue; break;
			default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Put()");
			};
		}
	}
}
