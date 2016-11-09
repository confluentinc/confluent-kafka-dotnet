using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;

namespace RdKafka.Internal
{
    enum ConfRes {
        Unknown = -2, /* Unknown configuration name. */
        Invalid = -1, /* Invalid configuration value. */
        Ok = 0        /* Configuration okay */
    }

    class SafeConfigHandle : SafeHandleZeroIsInvalid
    {
        private SafeConfigHandle()
        {
        }

        internal static SafeConfigHandle Create()
        {
            var ch = LibRdKafka.conf_new();
            if (ch.IsInvalid)
            {
                throw new Exception("Failed to create config");
            }
            return ch;
        }

        protected override bool ReleaseHandle()
        {
            LibRdKafka.conf_destroy(handle);
            return true;
        }

        internal IntPtr Dup() => LibRdKafka.conf_dup(handle);

        internal Dictionary<string, string> Dump()
        {
            UIntPtr cntp = (UIntPtr) 0;
            IntPtr data = LibRdKafka.conf_dump(handle, out cntp);

            if (data == IntPtr.Zero)
            {
                throw new Exception("Zero data");
            }

            try
            {
                if (((int) cntp & 1) != 0)
                {
                    // Expect Key -> Value, so even number of strings
                    throw new Exception("Invalid number of config entries");
                }

                var dict = new Dictionary<string, string>();
                for (int i = 0; i < (int) cntp / 2; i++)
                {
                    dict.Add(Marshal.PtrToStringAnsi(Marshal.ReadIntPtr(data, 2 * i * Marshal.SizeOf<IntPtr>())),
                             Marshal.PtrToStringAnsi(Marshal.ReadIntPtr(data, (2 * i + 1) * Marshal.SizeOf<IntPtr>())));
                }
                // Filter out callback pointers
                return dict.Where(kv => !kv.Key.EndsWith("_cb")).ToDictionary(kv => kv.Key, kv => kv.Value);
            }
            finally
            {
                LibRdKafka.conf_dump_free(data, cntp);
            }
        }

        internal void Set(string name, string value)
        {
            // TODO: Constant instead of 512?
            var errorStringBuilder = new StringBuilder(512);
            ConfRes res = LibRdKafka.conf_set(handle, name, value,
                    errorStringBuilder, (UIntPtr) errorStringBuilder.Capacity);
            if (res == ConfRes.Ok)
            {
                return;
            }
            else if (res == ConfRes.Invalid)
            {
                throw new ArgumentException(errorStringBuilder.ToString());
            }
            else if (res == ConfRes.Unknown)
            {
                throw new InvalidOperationException(errorStringBuilder.ToString());
            }
            else
            {
                throw new Exception("Unknown error while setting configuration property");
            }
        }

        internal string Get(string name)
        {
            UIntPtr destSize = (UIntPtr) 0;
            StringBuilder sb = null;

            ConfRes res = LibRdKafka.conf_get(handle, name, null, ref destSize);
            if (res == ConfRes.Ok)
            {
                sb = new StringBuilder((int) destSize);
                res = LibRdKafka.conf_get(handle, name, sb, ref destSize);
            }
            if (res != ConfRes.Ok)
            {
                if (res == ConfRes.Unknown)
                {
                    throw new InvalidOperationException($"No such configuration property: {name}");
                }
                throw new Exception("Unknown error while getting configuration property");
            }
            return sb?.ToString();
        }
    }
}
