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
using System.Linq;
using System.Runtime.Serialization;

namespace Confluent.SchemaRegistry
{
    [DataContract]
    public class RuleSet : IEquatable<RuleSet>
    {
        [DataMember(Name = "migrationRules")]
        public IList<Rule> MigrationRules { get; set; }
        
        [DataMember(Name = "domainRules")]
        public IList<Rule> DomainRules { get; set; }

        /// <summary>
        ///     Empty constructor for serialization
        /// </summary>
        private RuleSet() { }

        public RuleSet(IList<Rule> migrationRules, IList<Rule> domainRules)
        {
            MigrationRules = migrationRules;
            DomainRules = domainRules;
        }
        
      public bool HasRules(RuleMode mode) {
        switch (mode) {
          case RuleMode.Upgrade:
          case RuleMode.Downgrade:
            return MigrationRules.Any(r => r.Mode == mode || r.Mode == RuleMode.UpDown);
          case RuleMode.UpDown:
            return MigrationRules.Any(r => r.Mode == mode);
          case RuleMode.Write:
          case RuleMode.Read:
            return DomainRules.Any(r => r.Mode == mode || r.Mode == RuleMode.Write);
          case RuleMode.WriteRead:
            return DomainRules.Any(r => r.Mode == mode);
          default:
            return false;
        }
      }

      public bool Equals(RuleSet other)
      {
        if (ReferenceEquals(null, other)) return false;
        if (ReferenceEquals(this, other)) return true;
        return Equals(MigrationRules, other.MigrationRules) && Equals(DomainRules, other.DomainRules);
      }

      public override bool Equals(object obj)
      {
        if (ReferenceEquals(null, obj)) return false;
        if (ReferenceEquals(this, obj)) return true;
        if (obj.GetType() != this.GetType()) return false;
        return Equals((RuleSet)obj);
      }

      public override int GetHashCode()
      {
        unchecked
        {
          return ((MigrationRules != null ? MigrationRules.GetHashCode() : 0) * 397) ^ 
                 (DomainRules != null ? DomainRules.GetHashCode() : 0);
        }
      }
    }
}
