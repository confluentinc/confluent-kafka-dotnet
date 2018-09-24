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


namespace Confluent.Kafka
{
    /// <summary>
    ///     OnLog callback event handler implementations.
    /// </summary>
    /// <remarks>
    ///     Warning: Log handlers are called spontaneously from internal librdkafka 
    ///     threads and the application must not call any Confluent.Kafka APIs from 
    ///     within a log handler or perform any prolonged operations.
    /// </remarks>
    public static class Loggers
    {
        /// <summary>
        ///     The method used to log messages by default.
        /// </summary>
        public static void ConsoleLogger(LogMessage logInfo)
        {
            var now = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");
            Console.Error.WriteLine($"{logInfo.Level}|{now}|{logInfo.Name}|{logInfo.Facility}| {logInfo.Message}");
        }
    }
}
