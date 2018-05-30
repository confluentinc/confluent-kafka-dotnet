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

using System;


namespace Confluent.Kafka.Admin
{
    /// <summary>
    ///     Options for the CreateTopics method.
    /// </summary>
    public class CreateTopicsOptions
    {
        /// <summary>
        ///     If true, the request should be validated only without creating the topic.
        /// 
        ///     Default: false
        /// </summary>
        public bool ValidateOnly { get; set; } = false;

        /// <summary>
        ///     The request timeout in milliseconds for this operation or null if the
        ///     default request timeout for the AdminClient should be used.
        /// 
        ///     Default: null
        /// </summary>
        public TimeSpan? Timeout { get; set; }
    }
}