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

using System;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Extension methods for the <see cref="TimeSpan"/> class.
    /// </summary>
    internal static class TimeSpanExtensions
    {
        /// <summary>
        ///     Converts the TimeSpan value <paramref name="timespan" /> to an integer number of milliseconds.
        ///     An <see cref="OverflowException"/> is thrown if the number of milliseconds is greater than Int32.MaxValue.
        /// </summary>
        /// <param name="timespan">
        ///     The TimeSpan value to convert to milliseconds.
        /// </param>
        /// <returns>
        ///     The TimeSpan value <paramref name="timespan" /> in milliseconds.
        /// </returns>
        internal static int TotalMillisecondsAsInt(this TimeSpan timespan)
        {
            int millisecondsTimespan;
            checked { millisecondsTimespan = (int)timespan.TotalMilliseconds; }
            return millisecondsTimespan;
        }
    }
}
