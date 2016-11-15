using System;
using System.Collections.Generic;


namespace Confluent.Kafka.Internal
{
    /// <summary>
    ///     A minimal Dictionary implementation with the following properties:
    ///         1. all access is thread safe.
    ///         2. grows unbounded (elements cannot be removed).
    ///         3. reads are fast (no locking).
    ///         4. writes are slow (locking).
    ///         5. is Disposable (+ values must themselves be Disposable).
    /// </summary>
    internal sealed class SafeDictionary<TKey, TValue> : IDisposable where TValue : IDisposable
    {
        private volatile Dictionary<TKey, TValue> readDictionary = new Dictionary<TKey, TValue>();
        private Object writeLockObj = new Object();

        public TValue this[TKey name]
        {
            get
            {
                return readDictionary[name];
            }
        }

        public void Add(TKey key, TValue val)
        {
            lock (writeLockObj)
            {
                Dictionary<TKey, TValue> writeDictionary = new Dictionary<TKey, TValue>(readDictionary);
                writeDictionary.Add(key, val);
                // this is atomic.
                readDictionary = writeDictionary;
            }
        }

        public bool ContainsKey(TKey key) => readDictionary.ContainsKey(key);

        public void Dispose()
        {
            foreach (var kv in readDictionary)
            {
                kv.Value.Dispose();
            }
        }
    }
}
