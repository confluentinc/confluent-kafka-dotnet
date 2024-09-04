// Copyright 2024 Confluent Inc.
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

using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Threading.Tasks;

namespace Confluent.SchemaRegistry
{
    /// <summary>
    ///     An error action   
    /// </summary>
    public class ErrorAction : IRuleAction
    {
        public static readonly string ActionType = "ERROR";

        public void Configure(IEnumerable<KeyValuePair<string, string>> config)
        {
        }
        
        public string Type()
        {
            return ActionType;
        }
        
        public Task Run(RuleContext ctx, object message, RuleException exception = null)
        {
            string msg = "Rule failed: " + ctx.Rule.Name;
            throw new SerializationException(msg, exception);
        }

        public void Dispose()
        {
        }
    }
}
