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
        // TODO: Implement better behavior if this class is used after disposal?
        // It's not public, so I think this definitely doesn't matter.

        private volatile Dictionary<TKey, TValue> readDictionary = new Dictionary<TKey, TValue>();
        private Dictionary<TKey, TValue> writeDictionary = new Dictionary<TKey, TValue>();
        private bool Disposed { get { return writeDictionary == null; } }
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
                writeDictionary.Add(key, val);
            }

            // this is atomic.
            readDictionary = writeDictionary;
        }

        public bool ContainsKey(TKey key) => readDictionary.ContainsKey(key);

        ~SafeDictionary()
        {
            Dispose(false);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public void Dispose(bool disposing)
        {
            // TODO: Think carefully about whether the implementation of this method is correct.
            if (this.Disposed)
            {
                return;
            }
            writeDictionary = null;

            foreach (var kv in readDictionary)
            {
                kv.Value.Dispose();
            }
            readDictionary = null;
        }
    }
}
