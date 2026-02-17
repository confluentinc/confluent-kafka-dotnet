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
using System.Threading.Tasks;

#if NET8_0_OR_GREATER
using System.Buffers.Text;
#endif

namespace Confluent.SchemaRegistry
{
    public static class Utils
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

        /// <summary>
        /// Asynchronously transforms each element of an IEnumerable&lt;T&gt;,
        /// passing in its zero-based index and the element itself, using a
        /// Func&lt;int, object, Task&lt;object&gt;&gt;, and returns a List&lt;T&gt; of the transformed elements.
        /// </summary>
        /// <param name="sourceEnumerable">
        ///   An object implementing IEnumerable&lt;T&gt; for some T.
        /// </param>
        /// <param name="indexedTransformer">
        ///   A function that takes (index, element as object) and returns a Task whose Result
        ///   is the new element (as object). The returned object must be castable to T.
        /// </param>
        /// <returns>
        ///   A Task whose Result is a List&lt;T&gt; (boxed as object) containing all transformed elements.
        ///   Await and then cast back to IEnumerable&lt;T&gt; to enumerate.
        /// </returns>
        public static async Task<object> TransformEnumerableAsync(
            object sourceEnumerable,
            Func<int, object, Task<object>> indexedTransformer)
        {
            if (sourceEnumerable == null)
                throw new ArgumentNullException(nameof(sourceEnumerable));
            if (indexedTransformer == null)
                throw new ArgumentNullException(nameof(indexedTransformer));

            // 1. Find the IEnumerable<T> interface on the source object
            var srcType = sourceEnumerable.GetType();
            var enumInterface = srcType
                .GetInterfaces()
                .FirstOrDefault(i =>
                    i.IsGenericType &&
                    i.GetGenericTypeDefinition() == typeof(IEnumerable<>));

            if (enumInterface == null)
                throw new ArgumentException("Source must implement IEnumerable<T>", nameof(sourceEnumerable));

            // 2. Extract the element type T
            var elementType = enumInterface.GetGenericArguments()[0];

            // 3. Build a List<T> at runtime
            var listType   = typeof(List<>).MakeGenericType(elementType);
            var resultList = (IList)Activator.CreateInstance(listType);

            // 4. Transform each element sequentially to preserve field context stack integrity
            // (RuleContext.fieldContexts is a shared stack that is not thread-safe)
            int index = 0;
            foreach (var item in (IEnumerable)sourceEnumerable)
            {
                var transformed = await indexedTransformer(index, item).ConfigureAwait(false);
                resultList.Add(transformed);
                index++;
            }

            // 7. Return the List<T> as object
            return resultList;
        }

        /// <summary>
        /// Asynchronously transforms each value of an IDictionary&lt;K,V&gt;,
        /// by invoking the provided Func&lt;object, object, Task&lt;object&gt;&gt;
        /// passing in the key and the original value
        /// and returns a new Dictionary&lt;K,V&gt; whose values are the awaited results.
        /// </summary>
        /// <param name="sourceDictionary">
        ///   An object implementing IDictionary&lt;K,V&gt; for some K,V.
        /// </param>
        /// <param name="transformer">
        ///   A function that takes (key as object, value as object) and returns a Task whose Result
        ///   is the new value (as object). The returned object must be castable to V.
        /// </param>
        /// <returns>
        ///   A Task whose Result is a Dictionary&lt;K,V&gt; containing all the transformed values.
        ///   Await and then cast back to IDictionary&lt;K,V&gt; to enumerate.
        /// </returns>
        public static async Task<object> TransformDictionaryAsync(
            object sourceDictionary,
            Func<object, object, Task<object>> transformer)
        {
            if (sourceDictionary == null)
                throw new ArgumentNullException(nameof(sourceDictionary));
            if (transformer == null)
                throw new ArgumentNullException(nameof(transformer));

            // 1. Find the IDictionary<K,V> interface on the source object
            var srcType = sourceDictionary.GetType();
            var dictInterface = srcType
                .GetInterfaces()
                .FirstOrDefault(i =>
                    i.IsGenericType &&
                    i.GetGenericTypeDefinition() == typeof(IDictionary<,>));

            if (dictInterface == null)
                throw new ArgumentException("Source must implement IDictionary<K,V>", nameof(sourceDictionary));

            // 2. Extract K and V
            var genericArgs = dictInterface.GetGenericArguments();
            var keyType   = genericArgs[0];
            var valueType = genericArgs[1];

            // 3. Create a Dictionary<K,V> at runtime
            var resultDictType = typeof(Dictionary<,>).MakeGenericType(keyType, valueType);
            var resultDict     = (IDictionary)Activator.CreateInstance(resultDictType);

            // 4. Transform each entry sequentially to preserve field context stack integrity
            // (RuleContext.fieldContexts is a shared stack that is not thread-safe)
            var nonGenericDict = (IDictionary)sourceDictionary;

            foreach (DictionaryEntry entry in nonGenericDict)
            {
                var transformedValue = await transformer(entry.Key, entry.Value).ConfigureAwait(false);
                resultDict.Add(entry.Key, transformedValue);
            }

            // 7. Return boxed Dictionary<K,V>
            return resultDict;
        }
    }
}
