using System;
using System.Text;

namespace Confluent.Kafka.Internal
{
    internal static class Util
    {
        internal static class Marshal
        {
            /// <summary>
            ///     Interpret a zero terminated c string as UTF-8.
            /// </summary>
            public static string PtrToStringUTF8(IntPtr strPtr)
            {
                // TODO: Is there a built in / vectorized / better way to implement this?
                var length = 0;
                unsafe
                {
                    byte* pTraverse = (byte *)strPtr;
                    while (*pTraverse != 0) { pTraverse += 1; }
                    length = (int)(pTraverse - (byte*)strPtr);
                }
                var strBuffer = new byte[length];
                System.Runtime.InteropServices.Marshal.Copy(strPtr, strBuffer, 0, length);
                return Encoding.UTF8.GetString(strBuffer);
            }
        }
    }
}
