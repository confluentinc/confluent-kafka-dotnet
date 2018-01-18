// Copyright 2016-2017 Confluent Inc.
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

using System;
using System.Text;
using SystemMarshal = System.Runtime.InteropServices.Marshal;
using Unsafe = System.Runtime.CompilerServices.Unsafe;


namespace Confluent.Kafka.Internal
{
    internal static class Util
    {
        internal static class Marshal
        {
            /// <summary>
            ///     Interpret a zero terminated c string as UTF-8.
            /// </summary>
            public unsafe static string PtrToStringUTF8(IntPtr strPtr)
            {
                // TODO: Is there a built in / vectorized / better way to implement this?              
                byte* pTraverse = (byte*)strPtr;
                while (*pTraverse != 0) { pTraverse += 1; }
                var length = (int)(pTraverse - (byte*)strPtr);
#if NET45
                var strBuffer = new byte[length];
                System.Runtime.InteropServices.Marshal.Copy(strPtr, strBuffer, 0, length);
                return Encoding.UTF8.GetString(strBuffer);
#else
                // Avoid unnecessary data copying on NET45+
                return Encoding.UTF8.GetString((byte*)strPtr.ToPointer(), length);
#endif
            }

            /// <summary>
            ///     Reinterpret_cast without strings marshaling
            /// </summary>
            /// <typeparam name="T">
            ///     Type of struct to cast
            /// </typeparam>
            /// <param name="ptr">
            ///     Raw pointer to use
            /// </param>
            /// <returns>
            ///     A value of type <typeparamref name="T"/>
            /// </returns>
            public static unsafe T PtrToStructureUnsafe<T>(IntPtr ptr)
            {
                return Unsafe.Read<T>(ptr.ToPointer());
            }

            public static T PtrToStructure<T>(IntPtr ptr)
            {
#if NET45
                return (T)SystemMarshal.PtrToStructure(ptr, typeof(T));
#else
                return SystemMarshal.PtrToStructure<T>(ptr);
#endif
            }

            public static int SizeOf<T>()
            {
#if NET45
                return SystemMarshal.SizeOf(typeof(T));
#else
                return SystemMarshal.SizeOf<T>();
#endif
            }

            public static IntPtr OffsetOf<T>(string fieldName)
            {
#if NET45
                return SystemMarshal.OffsetOf(typeof(T), fieldName);
#else
                return SystemMarshal.OffsetOf<T>(fieldName);
#endif
            }
        }
    }
}
