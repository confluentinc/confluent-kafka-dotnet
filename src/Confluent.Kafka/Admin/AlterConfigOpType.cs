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


namespace Confluent.Kafka.Admin
{
    /// <summary>
    ///     Enum of allowed AlterConfigOpType.
    /// </summary>
    public enum AlterConfigOpType : int
    {
        /// <summary>
        ///     Sets/overwrites the configuration value.
        /// </summary>
        Set = 0,

        /// <summary>
        ///     Sets the configuration value to default or NULL.
        /// </summary>
        Delete = 1,

        /// <summary>
        ///     Appends the value to existing configuration values(only for list type values).
        /// </summary>
        Append = 2,

        /// <summary>
        ///     Subtracts the value from existing configuration values(only for list type values).
        /// </summary>
        Subtract = 3,
    };
}
