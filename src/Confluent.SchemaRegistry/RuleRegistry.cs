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
using System.Threading;

namespace Confluent.SchemaRegistry
{
    /// <summary>
    ///     A rule registry.
    /// </summary>
    public static class RuleRegistry
    {
        private static readonly SemaphoreSlim ruleExecutorsMutex = new SemaphoreSlim(1);
        private static readonly SemaphoreSlim ruleActionsMutex = new SemaphoreSlim(1);

        private static IDictionary<string, IRuleExecutor> ruleExecutors = new Dictionary<string, IRuleExecutor>();
        private static IDictionary<string, IRuleAction> ruleActions = new Dictionary<string, IRuleAction>();
        
        public static void RegisterRuleExecutor(IRuleExecutor executor)
        {
            ruleExecutorsMutex.Wait();
            try
            {
                if (!ruleExecutors.ContainsKey(executor.Type()))
                {
                    ruleExecutors.Add(executor.Type(), executor);
                }
            }
            finally
            {
                ruleExecutorsMutex.Release();
            }
        }
        
        public static bool TryGetRuleExecutor(string name, out IRuleExecutor executor)
        {
            ruleExecutorsMutex.Wait();
            try
            {
                return ruleExecutors.TryGetValue(name, out executor);
            }
            finally
            {
                ruleExecutorsMutex.Release();
            }
        }
        
        public static List<IRuleExecutor> GetRuleExecutors()
        {
            ruleExecutorsMutex.Wait();
            try
            {
                return new List<IRuleExecutor>(ruleExecutors.Values);
            }
            finally
            {
                ruleExecutorsMutex.Release();
            }
        }
        
        public static void RegisterRuleAction(IRuleAction action)
        {
            ruleActionsMutex.Wait();
            try
            {
                if (!ruleActions.ContainsKey(action.Type()))
                {
                    ruleActions.Add(action.Type(), action);
                }
            }
            finally
            {
                ruleActionsMutex.Release();
            }
        }
        
        public static bool TryGetRuleAction(string name, out IRuleAction action)
        {
            ruleActionsMutex.Wait();
            try
            {
                return ruleActions.TryGetValue(name, out action);
            }
            finally
            {
                ruleActionsMutex.Release();
            }
        }
        
        public static List<IRuleAction> GetRuleActions()
        {
            ruleActionsMutex.Wait();
            try
            {
                return new List<IRuleAction>(ruleActions.Values);
            }
            finally
            {
                ruleActionsMutex.Release();
            }
        }
    }
}
