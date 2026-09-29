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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace Confluent.SchemaRegistry
{
    /// <summary>
    ///     Determines when inline validation rules run, relative to domain rule
    ///     transformations.
    /// </summary>
    public enum ValidationRulesExecution
    {
        /// <summary>
        ///     Inline validation rules are not evaluated.
        /// </summary>
        Disabled,

        /// <summary>
        ///     Inline validation rules are evaluated on the original message, before
        ///     domain rule transformations.
        /// </summary>
        BeforeDomainRules,

        /// <summary>
        ///     Inline validation rules are evaluated on the transformed message, after
        ///     domain rules.
        /// </summary>
        AfterDomainRules
    }

    /// <summary>
    ///     An inline validation rule (a CHECK constraint) declared on a schema, either on
    ///     a record/message/object or on one of its fields.
    /// </summary>
    public class ValidationRule
    {
        /// <summary>
        ///     The rule name.
        /// </summary>
        [JsonProperty("name")]
        public string Name { get; set; }

        /// <summary>
        ///     Human readable documentation, used as the failure message when the rule
        ///     itself does not supply one.
        /// </summary>
        [JsonProperty("doc")]
        public string Doc { get; set; }

        /// <summary>
        ///     The rule expression.
        /// </summary>
        [JsonProperty("expr")]
        public string Expr { get; set; }

        /// <summary>
        ///     The equivalent SQL expression, if any. Carried for parity with the other
        ///     clients and used only in failure messages.
        /// </summary>
        [JsonProperty("sql")]
        public string Sql { get; set; }
    }

    /// <summary>
    ///     A single inline validation rule failure, located at
    ///     <see cref="FieldPath" /> within the message that was validated.
    /// </summary>
    public class ValidationRuleError
    {
        /// <summary>
        ///     The rule that failed.
        /// </summary>
        public ValidationRule Rule { get; }

        /// <summary>
        ///     The dotted path of the value that failed, empty for the root.
        /// </summary>
        public string FieldPath { get; }

        /// <summary>
        ///     An optional dynamic error message returned by the rule itself — set when the
        ///     rule expression returned a non-empty string explaining the failure (e.g.
        ///     <c>x &gt; 0 ? '' : 'x must be positive'</c>). Null when the failure was a
        ///     plain false or an evaluation error.
        /// </summary>
        public string Message { get; }

        /// <summary>
        ///     The underlying exception, when the rule failed to evaluate.
        /// </summary>
        public Exception Cause { get; }

        /// <summary>
        ///     Creates a new validation rule error.
        /// </summary>
        public ValidationRuleError(ValidationRule rule, string fieldPath, string message = null,
            Exception cause = null)
        {
            Rule = rule;
            FieldPath = fieldPath;
            Message = message;
            Cause = cause;
        }

        /// <summary>
        ///     Renders the failure as "path: name: detail", preferring the dynamic message
        ///     returned by the rule and falling back to its doc, SQL, then expression.
        /// </summary>
        public override string ToString()
        {
            string path = string.IsNullOrEmpty(FieldPath) ? "<root>" : FieldPath;
            string name = string.IsNullOrEmpty(Rule?.Name) ? "unnamed" : Rule.Name;
            string detail;
            if (!string.IsNullOrEmpty(Message))
            {
                detail = Message;
            }
            else if (!string.IsNullOrEmpty(Rule?.Doc))
            {
                detail = Rule.Doc;
            }
            else if (!string.IsNullOrEmpty(Rule?.Sql))
            {
                detail = Rule.Sql;
            }
            else
            {
                detail = Rule?.Expr;
            }

            string result = $"{path}: {name}: {detail}";
            if (Cause != null)
            {
                result += $" (caused by: {Cause.Message})";
            }

            return result;
        }
    }

    /// <summary>
    ///     Thrown when one or more inline validation rules fail during serialization.
    /// </summary>
    public class ValidationRulesFailedException : Exception
    {
        /// <summary>
        ///     Every violation found during the walk.
        /// </summary>
        public IList<ValidationRuleError> Violations { get; }

        /// <summary>
        ///     Creates a new exception aggregating the given violations.
        /// </summary>
        public ValidationRulesFailedException(IList<ValidationRuleError> violations)
            : base(BuildMessage(violations))
        {
            Violations = violations;
        }

        private static string BuildMessage(IList<ValidationRuleError> violations)
        {
            if (violations == null || violations.Count == 0)
            {
                return "Validation rule failed (no detail)";
            }

            var sb = new StringBuilder();
            sb.Append($"Validation rule failed ({violations.Count} violation");
            sb.Append(violations.Count == 1 ? "):" : "s):");
            foreach (var violation in violations)
            {
                sb.Append($"\n  - {violation}");
            }

            return sb.ToString();
        }
    }

    /// <summary>
    ///     Evaluates a single inline validation rule against a value.
    ///
    ///     Implementations return either a bool (false meaning the rule failed) or a string
    ///     (non-empty meaning the rule failed, with that string as the failure message).
    /// </summary>
    public interface IValidationRuleExecutor
    {
        /// <summary>
        ///     Evaluates the rule against the given value.
        /// </summary>
        /// <param name="rule">the rule to evaluate</param>
        /// <param name="schema">a schema hint describing the value</param>
        /// <param name="message">the value to validate</param>
        Task<object> Execute(ValidationRule rule, object schema, object message);
    }

    /// <summary>
    ///     Helpers shared by the per-format inline validation rule walkers.
    /// </summary>
    public static class ValidationRules
    {
        /// <summary>
        ///     The schema property (Avro) / keyword (JSON Schema) that holds inline
        ///     validation rules.
        /// </summary>
        public const string RulesProp = "confluent:rules";

        /// <summary>
        ///     Parses a "confluent:rules" property value from its JSON representation.
        ///     Missing or malformed values yield an empty list.
        /// </summary>
        public static IList<ValidationRule> Parse(string json)
        {
            if (string.IsNullOrEmpty(json))
            {
                return new List<ValidationRule>();
            }

            try
            {
                return JsonConvert.DeserializeObject<List<ValidationRule>>(json)
                       ?? new List<ValidationRule>();
            }
            catch (JsonException)
            {
                return new List<ValidationRule>();
            }
        }

        /// <summary>
        ///     Evaluates one inline validation rule, adding a
        ///     <see cref="ValidationRuleError" /> to <paramref name="violations" /> when it
        ///     fails. A rule that throws is itself recorded as a violation so the walk can
        ///     continue; a rule resolving to something other than a bool or string is a
        ///     programming error and propagates.
        /// </summary>
        public static async Task Evaluate(IValidationRuleExecutor executor, ValidationRule rule,
            object schema, object value, string path, IList<ValidationRuleError> violations)
        {
            object result;
            try
            {
                result = await executor.Execute(rule, schema, value).ConfigureAwait(false);
            }
            catch (RuleException e)
            {
                violations.Add(new ValidationRuleError(rule, path, null, e));
                return;
            }

            switch (result)
            {
                case bool b:
                    if (!b)
                    {
                        violations.Add(new ValidationRuleError(rule, path));
                    }

                    break;
                case string s:
                    if (s.Length > 0)
                    {
                        violations.Add(new ValidationRuleError(rule, path, s));
                    }

                    break;
                default:
                    throw new ArgumentException(
                        $"Validation rule '{rule.Name}' resolved to an unexpected type: " +
                        $"{result?.GetType().Name ?? "null"}");
            }
        }

        /// <summary>
        ///     Throws a single exception aggregating every violation found, or returns when
        ///     there are none.
        /// </summary>
        public static void ThrowIfFailed(IList<ValidationRuleError> violations)
        {
            if (violations != null && violations.Any())
            {
                throw new ValidationRulesFailedException(violations);
            }
        }
    }
}
