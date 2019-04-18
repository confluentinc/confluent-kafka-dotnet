// Copyright 2019 Confluent Inc.
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
using System.Collections.Generic;


namespace Confluent.Kafka
{
    /// <summary>
    ///     A builder class for <see cref="IAdminClient" /> instance
    ///     implementations that leverage an existing client handle.
    /// </summary>
    public class DependentAdminClientBuilder
    {
        /// <summary>
        ///     The configured client handle.
        /// </summary>
        public Handle Handle { get; set; }
        
        /// <summary>
        ///     An underlying librdkafka client handle that the AdminClient.
        /// </summary>
        public DependentAdminClientBuilder(Handle handle)
        {
            this.Handle = handle;
        }

        /// <summary>
        ///     Build a new IAdminClient implementation instance.
        /// </summary>
        public virtual IAdminClient Build()
        {
            return new AdminClient(this.Handle);
        }
    }
}
