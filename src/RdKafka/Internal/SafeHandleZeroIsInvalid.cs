using System;
using System.Runtime.InteropServices;

namespace RdKafka
{
    abstract class SafeHandleZeroIsInvalid : SafeHandle
    {
        internal SafeHandleZeroIsInvalid() : base(IntPtr.Zero, true) { }

        internal SafeHandleZeroIsInvalid(bool ownsHandle) : base(IntPtr.Zero, ownsHandle) { }

        public override bool IsInvalid => handle == IntPtr.Zero;

        protected override bool ReleaseHandle() => true;
    }
}
