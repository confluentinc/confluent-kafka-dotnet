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
    public class Error
    {
        public Error(ErrorCode code)
        {
            Code = code;
            reason = null;
        }

        public Error(ErrorCode code, string reason)
        {
            Code = code;
            this.reason = reason;
        }

        public ErrorCode Code { get; }

        private string reason;

        // Rich error string, will be empty for APIs
        // where there was just an ErrorCode. See ToString()
        public string Reason
        {
            get { return ToString(); }
        }

        public bool HasError
            => Code != ErrorCode.NoError;

        public bool IsLocalError
            => (int)Code < -1;

        public bool IsBrokerError
            => (int)Code > 0;

        public static implicit operator bool(Error e)
            => e.HasError;

        public static implicit operator ErrorCode(Error e)
            => e.Code;

        public static implicit operator Error(ErrorCode c)
            => new Error(c);

        public override bool Equals(object obj)
        {
            if (!(obj is Error))
            {
                return false;
            }

            return ((Error)obj).Code == Code;
        }

        public override int GetHashCode()
            => Code.GetHashCode();

        public static bool operator ==(Error a, Error b)
            => a.Equals(b);

        public static bool operator !=(Error a, Error b)
            => !(a == b);

        /// <summary>
        ///   Returns the string representation of the error.
        ///   Depending on error source this might be a rich
        ///   contextual error message, or a simple static
        ///   string representation of the error Code.
        /// </summary>
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
