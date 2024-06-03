// Copyright 2020 Confluent Inc.
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

// Disable obsolete warnings. ConstructValueSubjectName is still used a an internal implementation detail.
#pragma warning disable CS0618

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;


namespace Confluent.SchemaRegistry
{
    public abstract class AsyncSerde<TParsedSchema>
    {
        protected ISchemaRegistryClient schemaRegistryClient;
        protected IList<IRuleExecutor> ruleExecutors;
        
        protected bool useLatestVersion = false;
        protected bool latestCompatibilityStrict = false;
        protected IDictionary<string, string> useLatestWithMetadata = null;
        protected SubjectNameStrategyDelegate subjectNameStrategy = null;
        
        protected SemaphoreSlim serdeMutex = new SemaphoreSlim(1);
        
        private readonly IDictionary<Schema, TParsedSchema> parsedSchemaCache = new Dictionary<Schema, TParsedSchema>();
        private SemaphoreSlim parsedSchemaMutex = new SemaphoreSlim(1);
        
        protected AsyncSerde(ISchemaRegistryClient schemaRegistryClient, SerdeConfig config, IList<IRuleExecutor> ruleExecutors = null)
        {
            this.schemaRegistryClient = schemaRegistryClient;
            this.ruleExecutors = ruleExecutors ?? new List<IRuleExecutor>();

            if (config == null) { return; }
            
            foreach (IRuleExecutor executor in this.ruleExecutors.Concat(RuleRegistry.GetRuleExecutors()))
            {
                IEnumerable<KeyValuePair<string, string>> ruleConfigs = config
                    .Select(kv => new KeyValuePair<string, string>(
                        kv.Key.StartsWith("rules.") ? kv.Key.Substring("rules.".Length) : kv.Key, kv.Value));
                executor.Configure(ruleConfigs); 
            }
        }

        protected async Task<(Schema, TParsedSchema)> GetSchema(int writerId, string subject, string format = null)
        {
            Schema writerSchema = await schemaRegistryClient.GetSchemaBySubjectAsync(writerId, subject, format)
                .ConfigureAwait(continueOnCapturedContext: false);
            TParsedSchema parsedSchema = await GetParsedSchema(writerSchema);
            return (writerSchema, parsedSchema);
        }

        protected async Task<TParsedSchema> GetParsedSchema(Schema schema)
        {
            await parsedSchemaMutex.WaitAsync().ConfigureAwait(continueOnCapturedContext: false);
            try
            {
                if (!parsedSchemaCache.TryGetValue(schema, out TParsedSchema parsedSchema))
                {
                    if (parsedSchemaCache.Count > schemaRegistryClient.MaxCachedSchemas)
                    {
                        parsedSchemaCache.Clear();
                    }

                    parsedSchema = await ParseSchema(schema).ConfigureAwait(continueOnCapturedContext: false);
                    parsedSchemaCache[schema] = parsedSchema;
                }

                return parsedSchema;
            }
            finally
            {
                parsedSchemaMutex.Release();
            }
        }
        
        protected abstract Task<TParsedSchema> ParseSchema(Schema schema);
        
        protected async Task<IDictionary<string, string>> ResolveReferences(Schema schema)
        {
            IList<SchemaReference> references = schema.References;
            if (references == null)
            {
                return new Dictionary<string, string>();
            }

            IDictionary<string, string> result = new Dictionary<string, string>();
            ISet<string> visited = new HashSet<string>();
            result = await ResolveReferences(schema, result, visited)
                .ConfigureAwait(continueOnCapturedContext: false);
            return result;
        }
        
        private async Task<IDictionary<string, string>> ResolveReferences(
            Schema schema, IDictionary<string, string> schemas, ISet<string> visited)
        {
            IList<SchemaReference> references = schema.References;
            foreach (SchemaReference reference in references)
            {
                if (visited.Contains(reference.Name))
                {
                    continue;
                }

                visited.Add(reference.Name);
                if (!schemas.ContainsKey(reference.Name))
                {
                    Schema s = await schemaRegistryClient.GetRegisteredSchemaAsync(reference.Subject, reference.Version)
                        .ConfigureAwait(continueOnCapturedContext: false);
                    if (s == null)
                    {
                        throw new SerializationException("Could not find schema " + reference.Subject + "-" + reference.Version);
                    }
                    schemas[reference.Name] = s.SchemaString;
                    await ResolveReferences(s, schemas, visited)
                        .ConfigureAwait(continueOnCapturedContext: false);
                }
            }

            return schemas;
        }

        protected async Task<IList<Migration>> GetMigrations(string subject, Schema writerSchema, Schema readerSchema)
        {
            RuleMode migrationMode;
            Schema first;
            Schema last;
            IList<Migration> migrations = new List<Migration>();
            if (writerSchema.Version < readerSchema.Version)
            {
                migrationMode = RuleMode.Upgrade;
                first = writerSchema;
                last = readerSchema;
            }
            else if (writerSchema.Version > readerSchema.Version)
            {
                migrationMode = RuleMode.Downgrade;
                first = readerSchema;
                last = writerSchema;
            }
            else
            {
                return migrations;
            }

            IList<Schema> versions = await GetSchemasBetween(subject, first, last)
                .ConfigureAwait(continueOnCapturedContext: false);
            Schema previous = null;
            for (int i = 0; i < versions.Count; i++) {
              Schema current = versions[i];
              if (i == 0) {
                // skip the first version
                previous = current;
                continue;
              }
              if (current.RuleSet != null && current.RuleSet.HasRules(migrationMode)) {
                Migration m;
                if (migrationMode == RuleMode.Upgrade) {
                  m = new Migration(migrationMode, previous, current);
                } else {
                  m = new Migration(migrationMode, current, previous);
                }
                migrations.Add(m);
              }
              previous = current;
            }
            if (migrationMode == RuleMode.Downgrade)
            {
                migrations = migrations.Reverse().ToList();
            }
            return migrations;
        }

        private async Task<IList<Schema>> GetSchemasBetween(string subject, Schema first, Schema last)
        {
            if (last.Version - first.Version <= 1)
            {
                return new List<Schema> { first, last };
            }

            var tasks = new List<Task<RegisteredSchema>>();
            int version1 = first.Version;
            int version2 = last.Version;
            for (int i = version1 + 1; i < version2; i++) {
                tasks.Add(schemaRegistryClient.GetRegisteredSchemaAsync(subject, i));
            }
            RegisteredSchema[] schemas = await Task.WhenAll(tasks).ConfigureAwait(continueOnCapturedContext: false);

            var result = new List<Schema>();
            result.Add(first);
            result.AddRange(schemas);
            result.Add(last);
            return result;
        }
        
        protected async Task<RegisteredSchema> GetReaderSchema(string subject, Schema schema = null)
        {
            if (schemaRegistryClient == null)
            {
                return null;
            }
            if (useLatestWithMetadata != null && useLatestWithMetadata.Any())
            {
                return await schemaRegistryClient.GetLatestWithMetadataAsync(subject, useLatestWithMetadata, false)
                    .ConfigureAwait(continueOnCapturedContext: false);
            }
            if (useLatestVersion)
            {
                var latestSchema = await schemaRegistryClient.GetLatestSchemaAsync(subject)
                    .ConfigureAwait(continueOnCapturedContext: false);
                if (schema != null && latestCompatibilityStrict)
                {
                    var isCompatible = await schemaRegistryClient.IsCompatibleAsync(subject, schema)
                        .ConfigureAwait(continueOnCapturedContext: false);
                    if (!isCompatible)
                    {
                        throw new InvalidDataException("Schema not compatible with latest schema : " + latestSchema.SchemaString);
                    }
                }

                return latestSchema;
            }

            return null;
        }
        
        protected async Task<object> ExecuteMigrations(
            IList<Migration> migrations, 
            bool isKey,
            String subject, 
            String topic,
            Headers headers, 
            object message) 
        {
            foreach (Migration m in migrations)
            {
                message = await ExecuteRules(isKey, subject, topic, headers, m.RuleMode,
                    m.Source, m.Target, message, null).ConfigureAwait(continueOnCapturedContext: false);
            }
            return message;
        }

        /// <summary>
        ///     Execute rules 
        /// </summary>
        /// <param name="isKey"></param>
        /// <param name="subject"></param>
        /// <param name="topic"></param>
        /// <param name="headers"></param>
        /// <param name="ruleMode"></param>
        /// <param name="source"></param>
        /// <param name="target"></param>
        /// <param name="message"></param>
        /// <returns></returns>
        /// <exception cref="RuleConditionException"></exception>
        /// <exception cref="ArgumentException"></exception>
        protected async Task<object> ExecuteRules(
            bool isKey, 
            string subject, 
            string topic, 
            Headers headers,
            RuleMode ruleMode, 
            Schema source, 
            Schema target, 
            object message,
            FieldTransformer fieldTransformer)
        {
            if (message == null || target == null)
            {
                return message;
            }

            IList<Rule> rules;
            if (ruleMode == RuleMode.Upgrade)
            {
                rules = target.RuleSet?.MigrationRules;
            }
            else if (ruleMode == RuleMode.Downgrade)
            {
                // Execute downgrade rules in reverse order for symmetry
                rules = source.RuleSet?.MigrationRules.Reverse().ToList();
            }
            else
            {
                rules = target.RuleSet?.DomainRules;
                if (rules != null && ruleMode == RuleMode.Read)
                {
                    // Execute read rules in reverse order for symmetry
                    rules = rules.Reverse().ToList();
                }
            }

            if (rules == null)
            {
                return message;
            }

            for (int i = 0; i < rules.Count; i++)
            {
                Rule rule = rules[i];
                if (rule.Disabled)
                {
                    continue;
                }
                if (rule.Mode == RuleMode.WriteRead)
                {
                    if (ruleMode != RuleMode.Read && ruleMode != RuleMode.Write)
                    {
                        continue;
                    }
                }
                else if (rule.Mode == RuleMode.UpDown)
                {
                    if (ruleMode != RuleMode.Upgrade && ruleMode != RuleMode.Downgrade)
                    {
                        continue;
                    }
                }
                else if (ruleMode != rule.Mode)
                {
                    continue;
                }

                RuleContext ctx = new RuleContext(source, target,
                    subject, topic, headers, isKey, ruleMode, rule, i, rules, fieldTransformer);
                IRuleExecutor ruleExecutor = GetRuleExecutor(ruleExecutors, rule.Type.ToUpper());
                if (ruleExecutor != null)
                {
                    try
                    {
                        object result = await ruleExecutor.Transform(ctx, message)
                            .ConfigureAwait(continueOnCapturedContext: false);
                        switch (rule.Kind)
                        {
                            case RuleKind.Condition:
                                if (result is bool condition && !condition)
                                {
                                    throw new RuleConditionException(rule);
                                }

                                break;
                            case RuleKind.Transform:
                                message = result;
                                break;
                            default:
                                throw new ArgumentException("Unsupported rule kind " + rule.Kind);
                        }
                        await RunAction(ctx, ruleMode, rule, message != null ? rule.OnSuccess : rule.OnFailure,
                            message, null, message != null ? null : ErrorAction.ActionType)
                            .ConfigureAwait(continueOnCapturedContext: false);
                    }
                    catch (RuleException ex)
                    {
                        await RunAction(ctx, ruleMode, rule, rule.OnFailure, message, 
                            ex, ErrorAction.ActionType)
                            .ConfigureAwait(continueOnCapturedContext: false);
                    }
                }
                else
                {
                    await RunAction(ctx, ruleMode, rule, rule.OnFailure, message, 
                        new RuleException("Could not find rule executor of type " + rule.Type), ErrorAction.ActionType)
                        .ConfigureAwait(continueOnCapturedContext: false);
                }
            }
            return message;
        }

        private static IRuleExecutor GetRuleExecutor(IList<IRuleExecutor> ruleExecutors, string type)
        {
            if (ruleExecutors != null)
            {
                foreach (IRuleExecutor ruleExecutor in ruleExecutors)
                {
                    if (ruleExecutor.Type().Equals(type))
                    {
                        return ruleExecutor;
                    }
                }
            }

            if (RuleRegistry.TryGetRuleExecutor(type, out IRuleExecutor result))
            {
                return result;
            }

            return null;
        }

        private static async Task RunAction(RuleContext ctx, RuleMode ruleMode, 
            Rule rule, string action, object message, RuleException ex, string defaultAction)
        {
            string actionName = GetRuleActionName(rule, ruleMode, action);
            if (actionName == null)
            {
                actionName = defaultAction;
            }
            if (actionName != null)
            {
                IRuleAction ruleAction = GetRuleAction(actionName);
                if (ruleAction == null)
                {
                    throw new SerializationException("Could not find rule action of type " + actionName);
                }

                try
                {
                    await ruleAction.Run(ctx, message, ex).ConfigureAwait(continueOnCapturedContext: false);
                } catch (RuleException e)
                {
                    throw new SerializationException("Failed to run rule action " + actionName, e);
                }
            }
        }

        private static string GetRuleActionName(Rule rule, RuleMode ruleMode, string actionName)
        {
            if ((rule.Mode == RuleMode.WriteRead || rule.Mode == RuleMode.UpDown)
                && actionName != null
                && actionName.Contains(","))
            {
                String[] parts = actionName.Split(',');
                switch (ruleMode)
                {
                    case RuleMode.Write:
                    case RuleMode.Upgrade:
                        return parts[0];
                    case RuleMode.Read:
                    case RuleMode.Downgrade:
                        return parts[1];
                    default:
                        throw new ArgumentException("Unsupported rule mode " + ruleMode);
                }
            }
            return actionName;
        }

        private static IRuleAction GetRuleAction(string actionName)
        {
            if (actionName == ErrorAction.ActionType)
            {
                return new ErrorAction();
            }
            if (actionName == NoneAction.ActionType)
            {
                return new NoneAction();
            }
            RuleRegistry.TryGetRuleAction(actionName.ToUpper(), out IRuleAction action);
            return action;
        }
    }
    
    public class Migration : IEquatable<Migration>
    {
        public Migration(RuleMode ruleMode, Schema source, Schema target)
        {
            RuleMode = ruleMode;
            Source = source;
            Target = target;
        }
        
        public RuleMode RuleMode { get; set; }
        
        public Schema Source { get; set; }
        
        public Schema Target { get; set; }

        public bool Equals(Migration other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return RuleMode == other.RuleMode && Equals(Source, other.Source) && Equals(Target, other.Target);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((Migration)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (int)RuleMode;
                hashCode = (hashCode * 397) ^ (Source != null ? Source.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Target != null ? Target.GetHashCode() : 0);
                return hashCode;
            }
        }
    }
}
