// Copyright 2026 Confluent Inc.
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
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Avro.Generic;
using Avro.Specific;
using Cel.Checker;
using Cel.Common.Types.Pb;
using Cel.Tools;
using Google.Api.Expr.V1Alpha1;
using Google.Protobuf;
using Google.Protobuf.Reflection;
using NodaTime;

namespace Confluent.SchemaRegistry.Rules
{
    /// <summary>
    ///     A validation rule executor backed by CEL. Each rule expression is evaluated with
    ///     <c>this</c> bound to the value being validated and <c>now</c> bound to the
    ///     current time, and must return either a bool (false = failed) or a string
    ///     (non-empty = failed, with that string as the failure message).
    /// </summary>
    public class CelValidator : IValidationRuleExecutor
    {
        /// <summary>
        ///     Registers this validator as the global inline validation rule executor.
        /// </summary>
        public static void Register()
        {
            RuleRegistry.RegisterValidationRuleExecutor(new CelValidator());
        }

        private readonly CelExecutor executor = new CelExecutor();

        private readonly IDictionary<CelExecutor.RuleWithArgs, Script> cache =
            new Dictionary<CelExecutor.RuleWithArgs, Script>();

        private readonly SemaphoreSlim cacheMutex = new SemaphoreSlim(1);

        /// <summary>
        ///     Creates a new CEL validation rule executor.
        /// </summary>
        public CelValidator()
        {
        }

        /// <summary>
        ///     Evaluates a single validation rule against a value.
        /// </summary>
        /// <param name="rule">the rule to evaluate</param>
        /// <param name="schema">a hint describing the value. A protobuf
        ///     <see cref="FieldDescriptor" /> settles the CEL type from the field's own
        ///     declared type; anything else leaves it to be inferred from the value</param>
        /// <param name="message">the value to validate</param>
        public async Task<object> Execute(ValidationRule rule, object schema, object message)
        {
            string name = string.IsNullOrEmpty(rule.Name) ? "unnamed" : rule.Name;
            if (message == null)
            {
                // Walkers are expected to enforce skip-on-null before invoking the executor;
                // a null here means a non-compliant caller. Surface the contract violation
                // explicitly rather than trip a confusing CEL evaluation error.
                throw new RuleException(
                    $"Validation rule '{name}' received a null value; walkers must enforce " +
                    "skip-on-null before invoking the executor");
            }

            if (string.IsNullOrEmpty(rule.Expr))
            {
                throw new RuleException($"Validation rule '{name}' has no expression");
            }

            // Present the value the way its declared type implies before anything reads it:
            // the declared type, the script-type sample and the binding all derive from it,
            // and Cel.NET rejects a CLR enum outright ("enum not allowed here").
            message = CelExecutor.ToCelValue(message);

            // Prefer the field's declared type over the CLR type of the value: the
            // descriptor is what the rule was written against, and it distinguishes cases the
            // value cannot - an enum from an int, a uint64 from an int64. Falls back to the
            // value for a message, a map, or when no descriptor was supplied.
            Google.Api.Expr.V1Alpha1.Type thisType = null;
            if (schema is FieldDescriptor field)
            {
                thisType = CelExecutor.FindTypeForField(field);
                if (thisType != null)
                {
                    // The value has to be presented as that same type, or the two disagree
                    // and the rule fails at evaluation rather than answering.
                    message = CelExecutor.ToCelValueForField(field, message);
                }
            }

            thisType = thisType ?? CelExecutor.FindType(message);
            var declTypes = new Dictionary<string, Google.Api.Expr.V1Alpha1.Type>
            {
                { "this", thisType },
                { "now", Checked.CheckedTimestamp }
            };
            // A rule on a repeated or map field binds a collection to `this`, and the
            // registry has to be chosen from what the collection holds - otherwise the
            // elements' fields cannot be resolved at evaluation time.
            object typeSample = TypeSample(message);
            var ruleWithArgs = new CelExecutor.RuleWithArgs(
                rule.Expr, DetermineScriptType(typeSample), declTypes, schema?.ToString());

            Script script;
            await cacheMutex.WaitAsync().ConfigureAwait(false);
            try
            {
                if (!cache.TryGetValue(ruleWithArgs, out script))
                {
                    try
                    {
                        script = executor.BuildScript(ruleWithArgs, typeSample);
                    }
                    catch (Exception e)
                    {
                        throw new RuleException($"Could not compile validation rule '{name}'", e);
                    }

                    cache[ruleWithArgs] = script;
                }
            }
            finally
            {
                cacheMutex.Release();
            }

            var args = new Dictionary<string, object>
            {
                { "this", message },
                { "now", SystemClock.Instance.GetCurrentInstant() }
            };

            object result;
            try
            {
                result = script.Execute<object>(args);
            }
            catch (ScriptException e)
            {
                string detail = string.IsNullOrEmpty(rule.Doc) ? "" : $" ({rule.Doc})";
                throw new RuleException($"Could not execute validation rule '{name}'{detail}", e);
            }

            if (result is bool || result is string)
            {
                return result;
            }

            throw new RuleException(
                $"Validation rule '{name}' must return bool or string; got " +
                $"{result?.GetType().Name ?? "null"}");
        }

        /// <summary>
        ///     The value whose type determines the registry to evaluate with: the value
        ///     itself, or - when it is a collection of Avro records or protobuf messages -
        ///     the first element, whose type is the one that has to be registered. Anything
        ///     else, including an empty collection, is left as it is.
        /// </summary>
        private static object TypeSample(object message)
        {
            IEnumerable elements = message is IDictionary map ? map.Values : message as IList;
            if (elements == null)
            {
                return message;
            }

            foreach (object element in elements)
            {
                if (element is IMessage || element is ISpecificRecord || element is GenericRecord)
                {
                    return element;
                }

                break;
            }

            return message;
        }

        /// <summary>
        ///     Determines which CEL type registry a value needs, from the value itself.
        /// </summary>
        private static CelExecutor.ScriptType DetermineScriptType(object message)
        {
            if (message is ISpecificRecord || message is GenericRecord)
            {
                return CelExecutor.ScriptType.Avro;
            }

            if (message is IMessage)
            {
                return CelExecutor.ScriptType.Protobuf;
            }

            return CelExecutor.ScriptType.Json;
        }
    }
}
