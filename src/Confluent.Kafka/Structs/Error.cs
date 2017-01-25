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
    public struct Error
    {
        public Error(ErrorCode code)
        {
            Code = code;
            String = null;
        }

        public Error(ErrorCode code, string reason)
        {
            Code = code;
            String = reason;
        }

        public ErrorCode Code { get; }

        private string String; // Rich error string, will be empty for APIs
                               // where there was just an ErrorCode. See ToString()

        public string Message
            => ToString();

        public bool HasError
            => Code != ErrorCode.NO_ERROR;

        // TODO: questionably too tricky?
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

        // Returns the string representation of the error.
        // If a rich error string is available return that, otherwise fall
        // back to librdkafka's static error code to string conversion.
        public override string ToString()
        {
            if (!string.IsNullOrEmpty(String))
                return String;
            else
                return ErrorCode2String(Code);
        }


        // Return the human readable representation of a (librdkafka) error code
        static public string ErrorCode2String(ErrorCode code) => Internal.Util.Marshal.PtrToStringUTF8(Impl.LibRdKafka.err2str(code));
    }
}
