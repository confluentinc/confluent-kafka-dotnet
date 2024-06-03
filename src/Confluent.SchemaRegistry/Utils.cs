// Copyright 2024 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the 'License');
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an 'AS IS' BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;


namespace Confluent.SchemaRegistry
{
    public class Utils
    {
        public static bool DictEquals(IDictionary<string, string> a, IDictionary<string, string> b)
        {
            if (ReferenceEquals(a, b)) return true;
            if (a == null || b == null) return false;
            if (a.Count != b.Count) return false;
            foreach (var kvp in a)
            {
                if (!b.TryGetValue(kvp.Key, out var value)) return false;
                if (value != kvp.Value) return false;
            }
            return true;
        }
        
        public static bool DictEquals(IDictionary<string, ISet<string>> a, IDictionary<string, ISet<string>> b)
        {
            if (ReferenceEquals(a, b)) return true;
            if (a == null || b == null) return false;
            if (a.Count != b.Count) return false;
            foreach (var kvp in a)
            {
                if (!b.TryGetValue(kvp.Key, out var value)) return false;
                if (!SetEquals(value, kvp.Value)) return false;
            }
            return true;
        }

        public static bool SetEquals<T>(ISet<T> a, ISet<T> b)
        {
            if (ReferenceEquals(a, b)) return true;
            if (a == null || b == null) return false;
            if (a.Count != b.Count) return false;
            foreach (var item in a)
            {
                if (!b.Contains(item)) return false;
            }
            return true;
        }
        
        
        public static bool ListEquals<T>(IList<T> a, IList<T> b)
        {
            if (ReferenceEquals(a, b)) return true;
            if (a == null || b == null) return false;
            return a.SequenceEqual(b);
        }
    }
}
