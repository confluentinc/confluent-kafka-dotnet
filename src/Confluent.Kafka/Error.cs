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
    ///     Represents an error that occured when interacting with a
    ///     Kafka broker or the librdkafka library.
    /// </summary>
    public class Error
    {
        /// <summary>
        ///     Initialize a new Error instance from a particular
        ///     <see cref="ErrorCode"/> value.
        /// </summary>
        /// <param name="code">
        ///     The <see cref="ErrorCode"/> value associated with this Error.
        /// </param>
        /// <remarks>
        ///     The reason string associated with this Error will
        ///     be a static value associated with the <see cref="ErrorCode"/>.
        /// </remarks>
        public Error(ErrorCode code)
        {
            Code = code;
            reason = null;
        }

        /// <summary>
        ///     Initialize a new Error instance from a particular
        ///     <see cref="ErrorCode"/> value and custom <paramref name="reason"/>
        ///     string.
        /// </summary>
        /// <param name="code">
        ///     The <see cref="ErrorCode"/> value associated with this Error.
        /// </param>
        /// <param name="reason">
        ///     A custom reason string associated with the error
        ///     (overriding the static string associated with 
        ///     <paramref name="code"/>).
        /// </param>
        public Error(ErrorCode code, string reason)
        {
            Code = code;
            this.reason = reason;
        }

        /// <summary>
        ///     Gets the <see cref="ErrorCode"/> associated with this Error.
        /// </summary>
        public ErrorCode Code { get; }

        private string reason;

        /// <summary>
        ///     Gets a human readable reason string associated with this error.
        /// </summary>
        public string Reason
        {
            get { return ToString(); }
        }

        /// <summary>
        ///     true if Code != ErrorCode.NoError.
        /// </summary>
        public bool HasError
            => Code != ErrorCode.NoError;

        /// <summary>
        ///     true if this is error originated locally (within librdkafka), false otherwise.
        /// </summary>
        public bool IsLocalError
            => (int)Code < -1;

        /// <summary>
        ///     true if this error originated on a broker, false otherwise.
        /// </summary>
        public bool IsBrokerError
            => (int)Code > 0;

        /// <summary>
        ///     Converts the specified Error value to a boolean value (false if e.Code == ErrorCode.NoError, true otherwise).
        /// </summary>
        /// <param name="e">
        ///     The Error value to convert.
        /// </param>
        public static implicit operator bool(Error e)
            => e.HasError;

        /// <summary>
        ///     Converts the specified Error value to the value of it's Code property.
        /// </summary>
        /// <param name="e">
        ///     The Error value to convert.
        /// </param>
        public static implicit operator ErrorCode(Error e)
            => e.Code;

        /// <summary>
        ///     Converts the specified <see cref="ErrorCode"/> value to it's corresponding rich Error value.
        /// </summary>
        /// <param name="c">
        ///     The <see cref="ErrorCode"/> value to convert.
        /// </param>
        public static implicit operator Error(ErrorCode c)
            => new Error(c);

        /// <summary>
        ///     Tests whether this Error instance is equal to the specified object.
        /// </summary>
        /// <param name="obj">
        ///     The object to test.
        /// </param>
        /// <returns>
        ///     true if obj is an Error and the Code property values are equal. false otherwise.
        /// </returns>
        public override bool Equals(object obj)
        {
            if (!(obj is Error))
            {
                return false;
            }

            return ((Error)obj).Code == Code;
        }

        /// <summary>
        ///     Returns a hash code for this Error value.
        /// </summary>
        /// <returns>
        ///     An integer that specifies a hash value for this Error value.
        /// </returns>
        public override int GetHashCode()
            => Code.GetHashCode();

        /// <summary>
        ///     Tests whether Error value a is equal to Error value b.
        /// </summary>
        /// <param name="a">
        ///     The first Error value to compare.
        /// </param>
        /// <param name="b">
        ///     The second Error value to compare.
        /// </param>
        /// <returns>
        ///     true if Error values a and b are equal. false otherwise.
        /// </returns>
        public static bool operator ==(Error a, Error b)
        {
            if (object.ReferenceEquals(a, null))
            {
                return object.ReferenceEquals(b, null);
            }

            return a.Equals(b);
        }

        /// <summary>
        ///     Tests whether Error value a is not equal to Error value b.
        /// </summary>
        /// <param name="a">
        ///     The first Error value to compare.
        /// </param>
        /// <param name="b">
        ///     The second Error value to compare.
        /// </param>
        /// <returns>
        ///     true if Error values a and b are not equal. false otherwise.
        /// </returns>
        public static bool operator !=(Error a, Error b)
            => !(a == b);

        /// <summary>
        ///     Returns the string representation of the error.
        ///     Depending on error source this might be a rich
        ///     contextual error message, or a simple static
        ///     string representation of the error Code.
        /// </summary>
        /// <returns>
        ///     A string representation of the Error object.
        /// </returns>
        public override string ToString()
        {
            // If a rich error string is available return that, otherwise fall
            // back to librdkafka's static error code to string conversion.
            if (!string.IsNullOrEmpty(reason))
                return reason;
            else
                return Code.GetReason();
        }
    }
}
