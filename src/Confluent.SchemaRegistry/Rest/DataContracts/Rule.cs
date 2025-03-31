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
using System.Runtime.Serialization;
using Confluent.Shared.CollectionUtils;

namespace Confluent.SchemaRegistry
{
    [DataContract]
    public class Rule : IEquatable<Rule>
    {
        [DataMember(Name = "name")]
        public string Name { get; set; }
        
        [DataMember(Name = "doc")]
        public string Doc { get; set; }
        
        [DataMember(Name = "kind")]
        public RuleKind Kind { get; set; }
        
        [DataMember(Name = "mode")]
        public RuleMode Mode { get; set; }
        
        [DataMember(Name = "type")]
        public string Type { get; set; }
        
        [DataMember(Name = "tags")]
        public ISet<string> Tags { get; set; }
        
        [DataMember(Name = "params")]
        public IDictionary<string, string> Params { get; set; }
        
        [DataMember(Name = "expr")]
        public string Expr { get; set; }
        
        [DataMember(Name = "onSuccess")]
        public string OnSuccess { get; set; }
        
        [DataMember(Name = "onFailure")]
        public string OnFailure { get; set; }
        
        [DataMember(Name = "disabled")]
        public bool Disabled { get; set; }
        
        /// <summary>
        /// <summary>
        ///     Empty constructor for serialization
        /// </summary>
        private Rule() { }

        public Rule(string name, RuleKind kind, RuleMode mode, string type, ISet<string> tags,
            IDictionary<string, string> parameters)
        {
            Name = name;
            Kind = kind;
            Mode = mode;
            Type = type;
            Tags = tags;
            Params = parameters;
        }
        
        public Rule(string name, RuleKind kind, RuleMode mode, string type, ISet<string> tags, 
            IDictionary<string, string> parameters, string expr, string onSuccess, string onFailure, bool disabled)
        {
            Name = name;
            Kind = kind;
            Mode = mode;
            Type = type;
            Tags = tags;
            Params = parameters;
            Expr = expr;
            OnSuccess = onSuccess;
            OnFailure = onFailure;
            Disabled = disabled;
        }

        /// <inheritdoc />
        public bool Equals(Rule other)
        {
            return RuleEqualityComparer.Instance.Equals(this, other);
        }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as Rule);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return RuleEqualityComparer.Instance.GetHashCode(this);
        }
        
        private class RuleEqualityComparer : IEqualityComparer<Rule>
        {
            private readonly DictionaryEqualityComparer<string, string> paramsEqualityComparer = new();
            private readonly SetEqualityComparer<string> tagsEqualityComparer = new();
            
            private RuleEqualityComparer()
            {
            }
            
            public static RuleEqualityComparer Instance { get; } = new();
            
            public bool Equals(Rule x, Rule y)
            {
                if (ReferenceEquals(x, y)) return true;
                if (x is null) return false;
                if (y is null) return false;
                if (x.GetType() != y.GetType()) return false;
                if (x.Name != y.Name) return false;
                if (x.Doc != y.Doc) return false;
                if (x.Kind != y.Kind) return false;
                if (x.Mode != y.Mode) return false;
                if (x.Type != y.Type) return false;
                if (!tagsEqualityComparer.Equals(x.Tags, y.Tags)) return false;
                if (!paramsEqualityComparer.Equals(x.Params, y.Params)) return false;
                if (x.Expr != y.Expr) return false;
                if (x.OnSuccess != y.OnSuccess) return false;
                if (x.OnFailure != y.OnFailure) return false;
                if (x.Disabled != y.Disabled) return false;
                return true;
            }

            public int GetHashCode(Rule obj)
            {
                var hashCode = new HashCode();
                hashCode.Add(obj.Name);
                hashCode.Add(obj.Doc);
                hashCode.Add((int) obj.Kind);
                hashCode.Add((int) obj.Mode);
                hashCode.Add(obj.Type);
                hashCode.Add(obj.Tags, tagsEqualityComparer);
                hashCode.Add(obj.Params, paramsEqualityComparer);
                hashCode.Add(obj.Expr);
                hashCode.Add(obj.OnSuccess);
                hashCode.Add(obj.OnFailure);
                hashCode.Add(obj.Disabled);
                return hashCode.ToHashCode();
            }
        }
    }
}
