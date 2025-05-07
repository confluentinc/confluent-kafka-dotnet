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
using SystemGCHandle = System.Runtime.InteropServices.GCHandle;
using SystemGCHandleType = System.Runtime.InteropServices.GCHandleType;
#if NET5_0_OR_GREATER
using System.Diagnostics.CodeAnalysis;
#endif


namespace Confluent.Kafka.Internal
{
    internal static class Util
    {
        internal static class Marshal
        {
            /// <summary>
            ///     Convenience class for generating and pinning the UTF8
            ///     representation of a string.
            /// </summary>
            public class StringAsPinnedUTF8 : IDisposable
            {
                private SystemGCHandle gch;

                public StringAsPinnedUTF8(string str)
                {
                    byte[] strBytes = System.Text.UTF8Encoding.UTF8.GetBytes(str);
                    byte[] strBytesNulTerminated = new byte[strBytes.Length + 1]; // initialized to all 0's.
                    Array.Copy(strBytes, strBytesNulTerminated, strBytes.Length);
                    this.gch = SystemGCHandle.Alloc(strBytesNulTerminated, SystemGCHandleType.Pinned);
                }

                public IntPtr Ptr { get => this.gch.AddrOfPinnedObject(); }

                public void Dispose()
                {
                    gch.Free();
                }
            }

            /// <summary>
            ///     Interpret a zero terminated c string as UTF-8.
            /// </summary>
            public unsafe static string PtrToStringUTF8(IntPtr strPtr)
            {
                if (strPtr == IntPtr.Zero)
                {
                    return null;
                }
                
                // TODO: Is there a built in / vectorized / better way to implement this?              
                byte* pTraverse = (byte*)strPtr;
                while (*pTraverse != 0) { pTraverse += 1; }
                var length = (int)(pTraverse - (byte*)strPtr);

                return Encoding.UTF8.GetString((byte*)strPtr.ToPointer(), length);
            }

            public unsafe static string PtrToStringUTF8(IntPtr strPtr, UIntPtr strLength)
            {
                if (strPtr == IntPtr.Zero)
                {
                    return null;
                }

                return Encoding.UTF8.GetString((byte*)strPtr.ToPointer(), (int)strLength);
            }

            public static T PtrToStructure<
#if NET5_0_OR_GREATER
                [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.NonPublicConstructors)]
#endif
                T>(IntPtr ptr)
            {
                return SystemMarshal.PtrToStructure<T>(ptr);
            }

            public static int SizeOf<T>()
            {
                return SystemMarshal.SizeOf<T>();
            }

            public static IntPtr OffsetOf<T>(string fieldName)
            {
                return SystemMarshal.OffsetOf<T>(fieldName);
            }
        }
    }
}
