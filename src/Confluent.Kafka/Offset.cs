// Copyright 2016-2017 Confluent Inc., 2015-2016 Andreas Heider
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
// Derived from: rdkafka-dotnet, licensed under the 2-clause BSD License.
//
// Refer to LICENSE for more information.

namespace Confluent.Kafka
{
    /// <summary>
    ///     Represents a Kafka partition offset value.
    /// </summary>  
    /// <remarks>
    ///     This structure is the same size as a long - 
    ///     its purpose is to add some syntactical sugar 
    ///     related to special values.
    /// </remarks>
    public struct Offset
    {
        private const long RD_KAFKA_OFFSET_BEGINNING = -2;
        private const long RD_KAFKA_OFFSET_END = -1;
        private const long RD_KAFKA_OFFSET_STORED = -1000;
        private const long RD_KAFKA_OFFSET_INVALID = -1001;

        /// <summary>
        ///     A special value that refers to the beginning of a partition.
        /// </summary>
        /// <returns></returns>
        public static Offset Beginning { get { return new Offset(RD_KAFKA_OFFSET_BEGINNING); } }

        /// <summary>
        ///     A special value that refers to the end of a partition.
        /// </summary>
        /// <returns></returns>
        public static Offset End { get { return new Offset(RD_KAFKA_OFFSET_END); } }

        /// <summary>
        ///     A special value thet refers to the stored offset for a partition.
        /// </summary>
        /// <returns></returns>
        public static Offset Stored { get { return new Offset(RD_KAFKA_OFFSET_STORED); } }

        /// <summary>
        ///     A special value that refers to an invalid, unassigned or default partition offset.
        /// </summary>
        public static Offset Invalid { get { return new Offset(RD_KAFKA_OFFSET_INVALID); } }

        /// <summary>
        ///     Initializes a new instance of the Offset structure.
        /// </summary>
        /// <param name="offset">
        ///     The offset value
        /// </param>
        public Offset(long offset)
        {
            Value = offset;
        }

        /// <summary>
        ///     Gets the long value corresponding to this offset.
        /// </summary>
        public long Value { get; }

        /// <summary>
        ///     Gets whether or not this is one of the special 
        ///     offset values.
        /// </summary>
        public bool IsSpecial
        {
            get
            {
                return
                    Value == RD_KAFKA_OFFSET_BEGINNING ||
                    Value == RD_KAFKA_OFFSET_END ||
                    Value == RD_KAFKA_OFFSET_STORED ||
                    Value == RD_KAFKA_OFFSET_INVALID;
            }
        }

        public override bool Equals(object obj)
        {
            if (!(obj is Offset))
            {
                return false;
            }

            return ((Offset)obj).Value == this.Value;
        }

        public static bool operator ==(Offset a, Offset b)
            => a.Equals(b);

        public static bool operator !=(Offset a, Offset b)
            => !(a == b);

        public static bool operator >(Offset a, Offset b)
            => a.Value > b.Value;

        public static bool operator <(Offset a, Offset b)
            => a.Value < b.Value;

        public static bool operator >=(Offset a, Offset b)
            => a.Value >= b.Value;

        public static bool operator <=(Offset a, Offset b)
            => a.Value <= b.Value;

        public override int GetHashCode()
            => Value.GetHashCode();

        public static implicit operator Offset(long v)
            => new Offset(v);

        public static implicit operator long(Offset o)
            => o.Value;

        public override string ToString()
        {
            switch (Value)
            {
                case RD_KAFKA_OFFSET_BEGINNING:
                    return $"Beginning [{RD_KAFKA_OFFSET_BEGINNING}]";
                case RD_KAFKA_OFFSET_END:
                    return $"End [{RD_KAFKA_OFFSET_END}]";
                case RD_KAFKA_OFFSET_STORED:
                    return $"Stored [{RD_KAFKA_OFFSET_STORED}]";
                case RD_KAFKA_OFFSET_INVALID:
                    return $"Invalid [{RD_KAFKA_OFFSET_INVALID}]";
                default:
                    return Value.ToString();
            }
        }
    }
}
