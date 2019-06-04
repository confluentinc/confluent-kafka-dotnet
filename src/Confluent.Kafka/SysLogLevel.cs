// Copyright 2018 Confluent Inc.
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


namespace Confluent.Kafka
{
    /// <summary>
    ///     Represents enumeration with levels coming from syslog(3)
    /// </summary>
    public enum SyslogLevel
    {
        /// <summary>
        ///     System is unusable.
        /// </summary>
        Emergency = 0,

        /// <summary>
        ///     Action must be take immediately
        /// </summary>
        Alert = 1,

        /// <summary>
        ///     Critical condition.
        /// </summary>
        Critical = 2,

        /// <summary>
        ///     Error condition.
        /// </summary>
        Error = 3,

        /// <summary>
        ///     Warning condition.
        /// </summary>
        Warning = 4,

        /// <summary>
        ///     Normal, but significant condition.
        /// </summary>
        Notice = 5,
        
        /// <summary>
        ///     Informational message.
        /// </summary>
        Info = 6,
        
        /// <summary>
        ///     Debug-level message.
        /// </summary>
        Debug = 7
    }
}
