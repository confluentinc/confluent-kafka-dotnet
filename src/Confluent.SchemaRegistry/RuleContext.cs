// Copyright 2022 Confluent Inc.
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
using Confluent.Kafka;

namespace Confluent.SchemaRegistry
{
    /// <summary>
    ///     A rule context.
    /// </summary>
    public class RuleContext
    {
        public Schema Source { get; set; }

        public Schema Target { get; set; }

        public string Subject { get; set; }

        public string Topic { get; set; }

        public Headers Headers { get; set; }

        public bool IsKey { get; set; }

        public RuleMode RuleMode { get; set; }

        public Rule Rule { get; set; }

        public int Index { get; set; }

        public IList<Rule> Rules { get; set; }

        public FieldTransformer FieldTransformer { get; set; }
        public IDictionary<object, object> CustomData { get; } = new Dictionary<object, object>();

        private Stack<FieldContext> fieldContexts = new Stack<FieldContext>();

        public RuleContext(Schema source, Schema target, string subject, string topic, Headers headers, bool isKey,
            RuleMode ruleMode, Rule rule, int index, IList<Rule> rules, FieldTransformer fieldTransformer)
        {
            Source = source;
            Target = target;
            Subject = subject;
            Topic = topic;
            Headers = headers;
            IsKey = isKey;
            RuleMode = ruleMode;
            Rule = rule;
            Index = index;
            Rules = rules;
            FieldTransformer = fieldTransformer;
        }

        public ISet<string> GetTags(string fullName)
        {
            ISet<string> tags = new HashSet<string>();
            if (Target?.Metadata?.Tags != null)
            {
                foreach (var entry in Target?.Metadata?.Tags)
                {
                    if (WildcardMatcher.Match(fullName, entry.Key))
                    {
                        tags.UnionWith(entry.Value);
                    }
                }
            }

            return tags;
        }


        public string GetParameter(string key)
        {
            string value = null;
            Rule.Params?.TryGetValue(key, out value);
            if (value == null)
            {
                Target.Metadata?.Properties?.TryGetValue(key, out value);
            }

            return value;
        }


        public FieldContext CurrentField()
        {
            return fieldContexts.Count != 0 ? fieldContexts.Peek() : null;
        }

        public FieldContext EnterField(object containingMessage,
            string fullName, string name, Type type, ISet<string> tags)
        {
            ISet<string> allTags = new HashSet<string>(tags);
            allTags.UnionWith(GetTags(fullName));
            return new FieldContext(this, containingMessage, fullName, name, type, allTags);
        }

        public class FieldContext : IDisposable
        {
            public RuleContext RuleContext { get; set; }

            public object ContainingMessage { get; set; }

            public string FullName { get; set; }

            public string Name { get; set; }

            public Type Type { get; set; }

            public ISet<string> Tags { get; set; }

            public FieldContext(RuleContext ruleContext, object containingMessage, string fullName, string name,
                Type type, ISet<string> tags)
            {
                RuleContext = ruleContext;
                ContainingMessage = containingMessage;
                FullName = fullName;
                Name = name;
                Type = type;
                Tags = tags;
                RuleContext.fieldContexts.Push(this);
            }

            public bool IsPrimitive()
            {
                return Type == Type.String || Type == Type.Bytes || Type == Type.Int || Type == Type.Long ||
                       Type == Type.Float || Type == Type.Double || Type == Type.Boolean || Type == Type.Null;
            }

            public void Dispose()
            {
                RuleContext.fieldContexts.Pop();
            }
        }

        public enum Type
        {
            Record,
            Enum,
            Array,
            Map,
            Combined,
            Fixed,
            String,
            Bytes,
            Int,
            Long,
            Float,
            Double,
            Boolean,
            Null
        }
    }
}