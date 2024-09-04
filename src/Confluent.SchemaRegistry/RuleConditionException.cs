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

using System;

namespace Confluent.SchemaRegistry
{
    /// <summary>
    ///     A rule condition exception
    /// </summary>
    public class RuleConditionException : RuleException
    {
        /// <summary>
        ///    Constructor 
        /// </summary>
        public RuleConditionException()
        {
        }

        /// <summary>
        ///     Constructor
        /// </summary>
        /// <param name="rule"></param>
        public RuleConditionException(Rule rule) : base(getErrorMessage(rule))
        {
        }
        
        private static string getErrorMessage(Rule rule)
        {
            string errMsg = rule.Doc;
            if (string.IsNullOrEmpty(errMsg))
            {
                string expr = rule.Expr;
                errMsg = expr != null 
                    ? $"Expr failed: '{expr}'"
                    : $"Condition failed: '{rule.Name}'";
            }

            return errMsg;
        }
    }
}