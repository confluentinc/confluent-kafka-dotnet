using System.Collections.Generic;

namespace Confluent.Kafka
{
    /// <summary>
    ///     Extension methods for the <see cref="IDictionary{TKey,TValue}"/> class.
    /// </summary>
    internal static class DictionaryExtensions
    {
        internal static string[] ToStringArray(this IDictionary<string, string> dictionary)
        {
            if (dictionary == null) return null;
            if (dictionary.Count == 0) return new string[0];

            var result = new string[dictionary.Count * 2];
            var index = 0;
            foreach (var pair in dictionary)
            {
                result[index++] = pair.Key;
                result[index++] = pair.Value;
            }

            return result;
        }
    }
}