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
        }

        public ErrorCode Code { get; }

        // Note: In most practical scenarios there is no benefit to caching this +
        //       significant cost in keeping the extra string reference around.
        public string Message
            => Internal.Util.Marshal.PtrToStringUTF8(Impl.LibRdKafka.err2str(Code));

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

        public override string ToString()
            => $"[{(int)Code}] {Message}";

    }
}
