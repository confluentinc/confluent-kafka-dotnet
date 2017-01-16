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
    /// <remarks>
    ///     The container takes ownership of any resources placed in it. This is
    ///     a little atypical, but it avoids the need to expose a means for the
    ///     caller to iterate through the elements, which is difficult to do in
    //      a thread safe way.
    /// </remarks>
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
                if (readDictionary == null)
                {
                    throw new InvalidOperationException("Attempting to add an item to SafeDictionary which has been disposed.");
                }

                Dictionary<TKey, TValue> writeDictionary = new Dictionary<TKey, TValue>(readDictionary);
                writeDictionary.Add(key, val);
                // this is atomic.
                readDictionary = writeDictionary;
            }
        }

        public bool ContainsKey(TKey key)
            => readDictionary.ContainsKey(key);

        public void Dispose()
        {
            lock (writeLockObj)
            {
                foreach (var kv in readDictionary)
                {
                    kv.Value.Dispose();
                }
                readDictionary = null;
            }
        }
    }
}
