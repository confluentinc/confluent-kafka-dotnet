using System;
using System.Text;

namespace Confluent.Kafka
{
    public class StringNativeDeserializer : INativeDeserializer<string>
    {
        private readonly Encoding _encoding;

        public StringNativeDeserializer() : this(Encoding.UTF8)
        {
        }

        public StringNativeDeserializer(Encoding encoding)
        {
            _encoding = encoding;
        }

        public unsafe string Deserialize(IntPtr msgBuf, uint msgLen, IntPtr msgTopic)
        {
            var len = (int) msgLen;
#if NET45
            var strBuffer = new byte[len];
            System.Runtime.InteropServices.Marshal.Copy(msgBuf, strBuffer, 0, len);
            return _encoding.GetString(strBuffer);
#else
            return _encoding.GetString((byte*) msgBuf.ToPointer(), len);
#endif
        }
    }
}