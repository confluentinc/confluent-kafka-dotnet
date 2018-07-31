// Copyright 2016-2017 Confluent Inc., 2015-2016 Andreas Heider
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
// Derived from: rdkafka-dotnet, licensed under the 2-clause BSD License.
//
// Refer to LICENSE for more information.

using System;
using System.Runtime.InteropServices;


namespace Confluent.Kafka.Impl
{
    abstract class SafeHandleZeroIsInvalid : SafeHandle
    {
        private string handleName;

        internal SafeHandleZeroIsInvalid(string handleName) : base(IntPtr.Zero, true)
        {
            this.handleName = handleName;
        }

        internal SafeHandleZeroIsInvalid(string handleName, bool ownsHandle) : base(IntPtr.Zero, ownsHandle)
        {
            this.handleName = handleName;
        }

        public override bool IsInvalid => handle == IntPtr.Zero;
    }
}
