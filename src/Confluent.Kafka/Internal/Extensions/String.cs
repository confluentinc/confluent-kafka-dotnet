// Copyright 2016-2017 Confluent Inc.
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

#pragma warning disable SYSLIB0001 // utf7 insecure

using System;
using System.Text;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Extension methods for the <see cref="String"/> class.
    /// </summary>
    internal static class StringExtensions
    {
        internal static Encoding ToEncoding(this string encodingName) 
        {
            switch (encodingName.ToLower())
            {
                // these names match up with static properties of the Encoding class
                case "utf8":
                    return Encoding.UTF8;
                case "utf7":
                    return Encoding.UTF7;
                case "utf32":
                    return Encoding.UTF32;
                case "ascii":
                    return Encoding.ASCII;
                default:
                    return Encoding.GetEncoding(encodingName);
            }
        }

        internal static string Quote(this bool b) =>
                b ? "true" : "false";

        internal static string Quote(this string str) =>
                str == null ? "null" : $"\"{str.Replace("\"","\\\"")}\"";
    }
}
