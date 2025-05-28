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

// NOTE: This implementation is based on the original gist by David Fowler:
// https://gist.github.com/davidfowl/3dac8f7b3d141ae87abf770d5781feed

using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace Confluent.Shared.CollectionUtils;

/// <summary>
/// Extension methods for working with <see cref="ConcurrentDictionary{TKey, TValue}"/> 
/// where the values are asynchronous <see cref="Task{TResult}"/>.
/// </summary>
internal static class ConcurrentDictionaryExtensions
{
    /// <summary>
    /// Asynchronously gets the value associated with the specified key, or adds a new value produced by the specified asynchronous factory function.
    /// 
    /// Ensures that the factory function is only invoked once for each key, even when accessed concurrently.
    /// If the factory throws, the entry is removed from the dictionary to allow future retries.
    /// </summary>
    /// <typeparam name="TKey">The type of keys in the dictionary.</typeparam>
    /// <typeparam name="TValue">The type of values returned in the <see cref="Task{TValue}"/>.</typeparam>
    /// <param name="dictionary">The dictionary to operate on. Values must be of type <see cref="Task{TValue}"/>.</param>
    /// <param name="key">The key whose value to get or add.</param>
    /// <param name="valueFactory">The asynchronous factory function to generate a value if the key does not exist.</param>
    /// <returns>A task representing the asynchronous operation, with the resulting value.</returns>
    public static async Task<TValue> GetOrAddAsync<TKey, TValue>(
        this ConcurrentDictionary<TKey, Task<TValue>> dictionary,
        TKey key,
        Func<TKey, Task<TValue>> valueFactory)
    {
        while (true)
        {
            if (dictionary.TryGetValue(key, out var task))
            {
                return await task.ConfigureAwait(continueOnCapturedContext: false);
            }

            // This is the task that we'll return to all waiters. We'll complete it when the factory is complete
            var tcs = new TaskCompletionSource<TValue>(TaskCreationOptions.RunContinuationsAsynchronously);
            if (dictionary.TryAdd(key, tcs.Task))
            {
                try
                {
                    var value = await valueFactory(key).ConfigureAwait(continueOnCapturedContext: false);
                    tcs.TrySetResult(value);
                    return await tcs.Task.ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    // Propagate the exception to all awaiting consumers.
                    tcs.SetException(ex);

                    // Remove the entry to allow retries on failure.
                    dictionary.TryRemove(key, out _);
                    throw;
                }
            }
        }
    }
}