// Copyright 2024 Confluent Inc.
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

using System.Collections.Generic;

namespace Confluent.Shared.CollectionUtils;

internal class SetEqualityComparer<T> : IEqualityComparer<ISet<T>>
{
    private readonly IEqualityComparer<T> elementComparer;
    
    public SetEqualityComparer(IEqualityComparer<T> elementComparer)
    {
        this.elementComparer = elementComparer;
    }
    
    public SetEqualityComparer(): this(EqualityComparer<T>.Default)
    {
    }
    
    public bool Equals(ISet<T> x, ISet<T> y)
    {
        if (ReferenceEquals(x, y))
        {
            return true;
        }

        if (x is null || y is null)
        {
            return false;
        }

        if (x.Count != y.Count)
        {
            return false;
        }
        
        // If both sets use the same comparer, they're equal if they're the same
        // size and one is a "subset" of the other.
        if (x is HashSet<T> xHashSet && y is HashSet<T> yHashSet)
        {
            if (xHashSet.Comparer.Equals(yHashSet.Comparer))
            {
                foreach (var item in x)
                {
                    if (!y.Contains(item))
                    {
                        return false;
                    }
                }
                return true;
            }
        }
        
        // Otherwise, do an O(N^2) match.
        foreach (var yi in y)
        {
            bool found = false;
            foreach (T xi in x)
            {
                if (elementComparer.Equals(yi, xi))
                {
                    found = true;
                    break;
                }
            }

            if (!found)
            {
                return false;
            }
        }

        return true;
    }
    

    public int GetHashCode(ISet<T> obj)
    {
        // ReSharper disable once ConditionIsAlwaysTrueOrFalse
        if (obj is null)
        {
            return 0;
        }

        int hash = 0;
        foreach (var item in obj)
        {
            // Combine hash codes using XOR for set semantics (order-independent)
            hash ^= elementComparer.GetHashCode(item);
        }
        return hash;
    }
}