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

#if NET8_0_OR_GREATER
using System.Buffers.Text;
#endif

namespace Confluent.SchemaRegistry
{
    public class Utils
    {
        public static bool ListEquals<T>(IList<T> a, IList<T> b)
        {
            if (ReferenceEquals(a, b)) return true;
            if (a == null || b == null) return false;
            return a.SequenceEqual(b);
        }
        
        public static int IEnumerableHashCode<T>(IEnumerable<T> items)
        {
            if (items == null) return 0;

            var hash = 0;

            using (var enumerator = items.GetEnumerator())
            {
                while (enumerator.MoveNext())
                {
                    hash = (hash * 397) ^ (enumerator.Current?.GetHashCode() ?? 0);
                }
            }

            return hash;
        }

        internal static bool IsBase64String(string value)
        {
#if NET8_0_OR_GREATER
            return Base64.IsValid(value);
#else
            try
            {
                _ = Convert.FromBase64String(value);
                return true;
            }
            catch (FormatException)
            {
                return false;
            }
#endif
        }
    }
}
