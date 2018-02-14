// Copyright 2018 Confluent Inc.
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
using System.Collections.Generic;
using Xunit;


namespace Confluent.Kafka.UnitTests
{
    public class HeadersTests
    {
        [Fact]
        public void GetLast()
        {
            var hdrs = new Headers();
            hdrs.Add(new KeyValuePair<string, byte[]>("my-header", new byte[] { 42 }));
            hdrs.Add(new KeyValuePair<string, byte[]>("my-header", new byte[] { 44 }));
            hdrs.Add(new KeyValuePair<string, byte[]>("my-header-2", new byte[] { 45 }));

            Assert.Single(hdrs.GetLast("my-header"));
            Assert.Equal(44, hdrs.GetLast("my-header")[0]);
        }

        [Fact]
        public void GetLast_NotExist()
        {
            var hdrs = new Headers();
            hdrs.Add(new KeyValuePair<string, byte[]>("my-header", new byte[] { 42 }));

            Assert.Throws<KeyNotFoundException>(() => { hdrs.GetLast("my-header-2"); });
        }

        [Fact]
        public void TryGetLast()
        {
            var hdrs = new Headers();
            hdrs.Add(new KeyValuePair<string, byte[]>("my-header", new byte[] { 42 }));
            hdrs.Add(new KeyValuePair<string, byte[]>("my-header", new byte[] { 44 }));
            hdrs.Add(new KeyValuePair<string, byte[]>("my-header-2", new byte[] { 45 }));

            Assert.True(hdrs.TryGetLast("my-header", out byte[] val));
            Assert.Single(val);
            Assert.Equal(44, val[0]);
        }

        [Fact]
        public void TryGetLast_NotExist()
        {
            var hdrs = new Headers();
            hdrs.Add(new KeyValuePair<string, byte[]>("my-header", new byte[] { 42 }));

            Assert.False(hdrs.TryGetLast("my-header-2", out byte[] val));
        }

        [Fact]
        public void NullKey()
        {
            var hdrs = new Headers();
            Assert.Throws<ArgumentNullException>(() => hdrs.Add(null, new byte[] {}));
        }

        [Fact]
        public void NullValue()
        {
            var hdrs = new Headers();
            hdrs.Add(new KeyValuePair<string, byte[]>("my-header", null));
            Assert.Null(hdrs.GetLast("my-header"));
        }

        [Fact]
        public void Remove()
        {
            var hdrs = new Headers();
            hdrs.Add(new KeyValuePair<string, byte[]>("my-header", new byte[] { 42 }));
            hdrs.Add(new KeyValuePair<string, byte[]>("my-header", new byte[] { 44 }));
            hdrs.Add(new KeyValuePair<string, byte[]>("my-header-2", new byte[] { 45 }));

            hdrs.Remove("my-header");
            Assert.Single(hdrs);
            Assert.Equal("my-header-2", hdrs[0].Key);
            Assert.Equal(45, hdrs[0].Value[0]);
        }
    }
}
