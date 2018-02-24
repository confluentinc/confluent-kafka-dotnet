using System;
using System.Runtime.InteropServices;

namespace Confluent.Kafka
{
    public class PooledByteArraySegmentNativeDeserializer : INativeDeserializer<ArraySegment<byte>>
    {
        private readonly IBufferPool _bufferPool;

        public PooledByteArraySegmentNativeDeserializer(IBufferPool bufferPool)
        {
            _bufferPool = bufferPool;
        }

        public ArraySegment<byte> Deserialize(IntPtr msgBuf, uint msgLen, IntPtr msgTopic)
        {
            var len = (int) msgLen;

            byte[] buf = null;
            if (msgBuf != IntPtr.Zero)
                buf = _bufferPool.Rent(len);

            Marshal.Copy(msgBuf, buf, 0, len);

            return new ArraySegment<byte>(buf, 0, len);
        }
    }
}