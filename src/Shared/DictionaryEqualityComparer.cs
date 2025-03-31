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

internal class DictionaryEqualityComparer<TKey, TValue> : IEqualityComparer<IDictionary<TKey, TValue>>
#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
{
    private readonly IEqualityComparer<TValue> valueComparer;
    
    public DictionaryEqualityComparer(IEqualityComparer<TValue> valueComparer)
    {
        this.valueComparer = valueComparer;
    }

    public DictionaryEqualityComparer() : this(EqualityComparer<TValue>.Default)
    {
    }
    
    public bool Equals(IDictionary<TKey, TValue> x, IDictionary<TKey, TValue> y)
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
        
        foreach (var kvp in x)
        {
            if (!y.TryGetValue(kvp.Key, out var yValue))
            {
                return false;
            }
            
            if (!valueComparer.Equals(kvp.Value, yValue))
            {
                return false;
            }
        }

        return true;
    }

    public int GetHashCode(IDictionary<TKey, TValue> obj)
    {
        // ReSharper disable once ConditionIsAlwaysTrueOrFalse
        if (obj is null)
        {
            return 0;
        }

        int hash = 0;
        foreach (var kvp in obj)
        {
            int keyHash = kvp.Key?.GetHashCode() ?? 0;
            int valueHash = valueComparer.GetHashCode(kvp.Value);
            hash ^= keyHash ^ valueHash;
        }
        
        return hash;
    }
}