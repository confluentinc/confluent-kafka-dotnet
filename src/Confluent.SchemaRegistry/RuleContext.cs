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
        public RegisteredSchema Source { get; set; }

        public RegisteredSchema Target { get; set; }

        public string Subject { get; set; }

        public string Topic { get; set; }

        public Headers Headers { get; set; }

        public bool IsKey { get; set; }

        public RuleMode RuleMode { get; set; }

        public Rule Rule { get; set; }

        public IDictionary<object, object> CustomData { get; } = new Dictionary<object, object>();

        private Stack<FieldContext> fieldContexts = new Stack<FieldContext>();

        public RuleContext(RegisteredSchema source, RegisteredSchema target, string subject, string topic,
            Headers headers, bool isKey, RuleMode ruleMode, Rule rule)
        {
            Source = source;
            Target = target;
            Subject = subject;
            Topic = topic;
            Headers = headers;
            IsKey = isKey;
            RuleMode = ruleMode;
            Rule = rule;
        }

        internal ISet<string> getAnnotations(string fullName)
        {
            ISet<string> annotations = new HashSet<string>();
            Metadata metadata = Target.Metadata;
            if (metadata != null && metadata.Annotations != null)
            {
                // TODO wildcard matching
                metadata.Annotations.TryGetValue(fullName, out var ann);
                if (ann != null)
                {
                    annotations.UnionWith(ann);
                }
            }

                /*
                  for (Map.Entry<String, SortedSet<String>> entry : metadata.getAnnotations().entrySet()) {
                    if (WildcardMatcher.match(fullName, entry.getKey())) {
                      annotations.addAll(entry.getValue());
                    }
                  }
                */

            return annotations;
        }


        public FieldContext CurrentField()
        {
            return fieldContexts.Peek();
        }

        public FieldContext EnterField(RuleContext ctx, object containingMessage,
            string fullName, string name, Type type, ISet<string> annotations)
        {
            ISet<string> allAnnotations = new HashSet<string>(annotations);
            allAnnotations.UnionWith(ctx.getAnnotations(fullName));
            return new FieldContext(ctx, containingMessage, fullName, name, type, allAnnotations);
        }

        public class FieldContext : IDisposable
        {
            public RuleContext RuleContext { get; set; }

            public object ContainingMessage { get; set; }

            public string FullName { get; set; }

            public string Name { get; set; }

            public Type Type { get; set; }

            public ISet<string> Annotations { get; set; }

            public FieldContext(RuleContext ruleContext, object containingMessage, string fullName, string name,
                Type type, ISet<string> annotations)
            {
                RuleContext = ruleContext;
                ContainingMessage = containingMessage;
                FullName = fullName;
                Name = name;
                Type = type;
                Annotations = annotations;
                RuleContext.fieldContexts.Push(this);
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