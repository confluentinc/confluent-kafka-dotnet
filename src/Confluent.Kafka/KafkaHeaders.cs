using System;
#if NET7_0_OR_GREATER
using System.Diagnostics.CodeAnalysis;
#endif
#if NET8_0_OR_GREATER
using System.Runtime.CompilerServices;
#endif

namespace Confluent.Kafka
{
    using System.Text;

    /// <summary>
    ///     A single Kafka message header: a name and a byte-payload value.
    /// </summary>
    public struct KafkaHeader
    {
        /// <summary>The header name.</summary>
        public string Name;

        /// <summary>The header value bytes.</summary>
        public ReadOnlyMemory<byte> Value;
    }

#if NET8_0_OR_GREATER
    [InlineArray(KafkaHeaders.InlineCapacity)]
    internal struct KafkaHeaderInline
    {
        private KafkaHeader _element0;
    }
#endif

    /// <remarks>
    ///     Because this is a non-readonly value type, mutations (<c>Add</c>) only
    ///     persist if the struct is accessed as a local variable, a field, or by
    ///     reference. Pass with <c>ref</c>/<c>in</c> when crossing method boundaries.
    /// </remarks>
    public struct KafkaHeaders
    {
        internal const int InlineCapacity = 8;

#if NET8_0_OR_GREATER
        private KafkaHeaderInline _inline;
#endif
        private KafkaHeader[] _overflow;

        /// <summary>The number of headers currently in the collection.</summary>
        public int Count { get; private set; }

#if NET7_0_OR_GREATER
        /// <summary>
        ///     Returns publicthe header at <paramref name="index"/> by read-only reference.
        /// </summary>
        public readonly ref readonly KafkaHeader this[int index]
        {
            [UnscopedRef]
            get
            {
#if NET8_0_OR_GREATER
                if (index < InlineCapacity)
                {
                    return ref _inline[index];
                }
                return ref _overflow[index - InlineCapacity];
#else
                return ref _overflow[index];
#endif
            }
        }
#else
        /// <summary>
        ///     Returns the header at <paramref name="index"/>.
        /// </summary>
        public readonly KafkaHeader this[int index] => _overflow[index];
#endif

        /// <summary>
        ///     Appends a header. The <paramref name="value"/> memory is stored by
        ///     reference; the caller must keep it valid until the enclosing produce
        ///     call returns.
        /// </summary>
        public void Add(string name, ReadOnlyMemory<byte> value)
        {
#if NET8_0_OR_GREATER
            if (Count < InlineCapacity)
            {
                ref var slot = ref _inline[Count];
                slot.Name = name;
                slot.Value = value;
                Count++;
                return;
            }
            var overflowIndex = Count - InlineCapacity;
#else
            var overflowIndex = Count;
#endif

            if (_overflow == null)
            {
                _overflow = new KafkaHeader[4];
            }
            else if (overflowIndex == _overflow.Length)
            {
                Array.Resize(ref _overflow, _overflow.Length * 2);
            }

            ref var overflowSlot = ref _overflow[overflowIndex];
            overflowSlot.Name = name;
            overflowSlot.Value = value;
            Count++;
        }

        /// <summary>
        ///     Appends all headers in <paramref name="headers"/> in order.
        /// </summary>
        public void Add(ReadOnlySpan<KafkaHeader> headers)
        {
            for (int i = 0; i < headers.Length; i++)
            {
                ref readonly var h = ref headers[i];
                Add(h.Name, h.Value);
            }
        }

        /// <summary>
        ///     Sets the value of the last header named <paramref name="name"/>, or
        ///     appends a new header if none exists. The <paramref name="value"/> memory
        ///     is stored by reference (same lifetime contract as
        ///     <see cref="Add(string, ReadOnlyMemory{byte})"/>).
        /// </summary>
        public void AddOrUpdate(string name, ReadOnlyMemory<byte> value)
        {
            for (int i = Count - 1; i >= 0; i--)
            {
#if NET7_0_OR_GREATER
                ref var slot = ref GetSlot(i);
                if (Ascii.Equals(slot.Name, name))
                {
                    slot.Value = value;
                    return;
                }
#else
                if (_overflow[i].Name.Equals(name, StringComparison.Ordinal))
                {
                    _overflow[i].Value = value;
                    return;
                }
#endif
            }
            Add(name, value);
        }

        /// <summary>
        ///     Upserts a header by name: equivalent to
        ///     <see cref="AddOrUpdate(string, ReadOnlyMemory{byte})"/>.
        /// </summary>
        public ReadOnlyMemory<byte> this[string name]
        {
            set => AddOrUpdate(name, value);
        }

#if NET7_0_OR_GREATER
        [UnscopedRef]
        private ref KafkaHeader GetSlot(int index)
        {
#if NET8_0_OR_GREATER
            if (index < InlineCapacity)
            {
                return ref _inline[index];
            }
            return ref _overflow[index - InlineCapacity];
#else
            return ref _overflow[index];
#endif
        }
#endif

        /// <summary>
        ///     Finds the last header with the given <paramref name="name"/> and
        ///     returns its value as a span. Returns false if no match is found.
        /// </summary>
        public readonly bool TryGetLastBytes(string name, out ReadOnlySpan<byte> bytes)
        {
            for (int i = Count - 1; i >= 0; i--)
            {
#if NET7_0_OR_GREATER
                ref readonly var h = ref this[i];
                if (Ascii.Equals(h.Name, name))
                {
#else
                var h = this[i];
                if (h.Name.Equals(name,  StringComparison.Ordinal)) 
                {
#endif
                    bytes = h.Value.Span;
                    return true;
                }
            }
            bytes = default;
            return false;
        }

#if NET7_0_OR_GREATER
        /// <summary>
        ///     Returns a ref-struct enumerator for <c>foreach</c> iteration. Available
        ///     on net7+ only (requires <see cref="UnscopedRefAttribute"/>).
        /// </summary>
        [UnscopedRef]
        public Enumerator GetEnumerator() => new Enumerator(ref this);

        /// <summary>
        ///     Ref-struct enumerator borrowing from the source. Mutating the source
        ///     during iteration is undefined.
        /// </summary>
        public ref struct Enumerator
        {
            private readonly ref KafkaHeaders source;
            private int index;

            internal Enumerator(ref KafkaHeaders source)
            {
                this.source = ref source;
                this.index = -1;
            }

            /// <summary>The current header.</summary>
            public KafkaHeader Current => source[index];

            /// <summary>Advances to the next header. Returns false past the end.</summary>
            public bool MoveNext()
            {
                index++;
                return index < source.Count;
            }
        }
#endif
    }
}
