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

        public bool Equals(Rule other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Name == other.Name && Doc == other.Doc && Kind == other.Kind && Mode == other.Mode &&
                   Type == other.Type && Equals(Tags, other.Tags) && Equals(Params, other.Params) &&
                   Expr == other.Expr && OnSuccess == other.OnSuccess && OnFailure == other.OnFailure &&
                   Disabled == other.Disabled;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((Rule)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (Name != null ? Name.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Doc != null ? Doc.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (int)Kind;
                hashCode = (hashCode * 397) ^ (int)Mode;
                hashCode = (hashCode * 397) ^ (Type != null ? Type.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Tags != null ? Tags.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Params != null ? Params.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Expr != null ? Expr.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (OnSuccess != null ? OnSuccess.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (OnFailure != null ? OnFailure.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ Disabled.GetHashCode();
                return hashCode;
            }
        }
    }
}
