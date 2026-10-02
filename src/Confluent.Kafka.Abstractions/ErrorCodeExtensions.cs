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

using System;
using System.Threading;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Provides extension methods on the ErrorCode enumeration.
    /// </summary>
    public static class ErrorCodeExtensions
    {
        /// <summary>
        ///     Delegate registered by Confluent.Kafka after librdkafka has been
        ///     successfully initialized.
        /// </summary>
        private static Func<ErrorCode, string> _getReasonImpl;

        internal static void RegisterGetReasonImplementation(Func<ErrorCode, string> implementation)
        {
            if (implementation == null)
            {
                throw new ArgumentNullException(nameof(implementation));
            }

            Interlocked.CompareExchange(ref _getReasonImpl, implementation, null);
        }

        /// <summary>
        ///     Returns the error string associated with the particular
        ///     <see cref="ErrorCode"/> value.
        ///     After librdkafka has been initialized, the string comes from
        ///     librdkafka via rd_kafka_err2str(). Before initialization, the enum
        ///     member name is returned.
        /// </summary>
        public static string GetReason(this ErrorCode code)
        {
            var getReasonImpl = Volatile.Read(ref _getReasonImpl);
            return getReasonImpl == null ? code.ToString() : getReasonImpl(code);
        }
    }
}
