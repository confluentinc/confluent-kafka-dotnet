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
        public void Add()
        {
            var hdrs = new Headers();
            hdrs.Add("aaa", new byte[] { 32, 42 } );

            Assert.Single(hdrs);
            Assert.Equal("aaa", hdrs[0].Key);
            Assert.Equal(new byte[] {32, 42}, hdrs[0].GetValueBytes());
        }

        [Fact]
        public void AddHeader()
        {
            var hdrs = new Headers();
            hdrs.Add(new Header("bbb", new byte[] { 1, 2, 3 }));

            Assert.Single(hdrs);
            Assert.Equal("bbb", hdrs[0].Key);
            Assert.Equal(new byte[] { 1, 2, 3 }, hdrs[0].GetValueBytes());
        }

        [Fact]
        public void GetLast()
        {
            var hdrs = new Headers();
            hdrs.Add(new Header("my-header", new byte[] { 42 }));
            hdrs.Add(new Header("my-header", new byte[] { 44 }));
            hdrs.Add(new Header("my-header-2", new byte[] { 45 }));

            Assert.Single(hdrs.GetLastBytes("my-header"));
            Assert.Equal(44, hdrs.GetLastBytes("my-header")[0]);
        }

        [Fact]
        public void GetLast_NotExist()
        {
            var hdrs = new Headers();
            hdrs.Add(new Header("my-header", new byte[] { 42 }));

            Assert.Throws<KeyNotFoundException>(() => { hdrs.GetLastBytes("my-header-2"); });
        }

        [Fact]
        public void TryGetLast()
        {
            var hdrs = new Headers();
            hdrs.Add(new Header("my-header", new byte[] { 42 }));
            hdrs.Add(new Header("my-header", new byte[] { 44 }));
            hdrs.Add(new Header("my-header-2", new byte[] { 45 }));

            Assert.True(hdrs.TryGetLastBytes("my-header", out byte[] val));
            Assert.Single(val);
            Assert.Equal(44, val[0]);
        }

        [Fact]
        public void TryGetLast_NotExist()
        {
            var hdrs = new Headers();
            hdrs.Add(new Header("my-header", new byte[] { 42 }));

            Assert.False(hdrs.TryGetLastBytes("my-header-2", out byte[] val));
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
            hdrs.Add(new Header("my-header", null));
            Assert.Null(hdrs.GetLastBytes("my-header"));
        }

        [Fact]
        public void Remove()
        {
            var hdrs = new Headers();
            hdrs.Add(new Header("my-header", new byte[] { 42 }));
            hdrs.Add(new Header("my-header", new byte[] { 44 }));
            hdrs.Add(new Header("my-header-2", new byte[] { 45 }));

            hdrs.Remove("my-header");
            Assert.Single(hdrs);
            Assert.Equal("my-header-2", hdrs[0].Key);
            Assert.Equal(45, hdrs[0].GetValueBytes()[0]);
        }

        [Fact]
        public void Indexer()
        {
            var h1 = new Header("my-header", new byte[] { 42 });
            var h2 = new Header("my-header", new byte[] { 44 });
            var h3 = new Header("my-header-2", new byte[] { 45 });

            var hdrs = new Headers();
            hdrs.Add(h1);
            hdrs.Add(h2);
            hdrs.Add(h3);

            Assert.Equal(h1, hdrs[0]);
            Assert.Equal(h2, hdrs[1]);
            Assert.Equal(h3, hdrs[2]);
        }

        [Fact]
        public void Count()
        {
            var hdrs = new Headers();
            Assert.Equal(0, hdrs.Count);

            hdrs.Add(new Header("my-header", new byte[] { 42 }));
            hdrs.Add(new Header("my-header", new byte[] { 44 }));
            hdrs.Add(new Header("my-header-2", new byte[] { 45 }));

            Assert.Equal(3, hdrs.Count);
        }

        [Fact]
        public void Enumerator()
        {
            var hdrs = new Headers();
            hdrs.Add(new Header("my-header", new byte[] { 42 }));
            hdrs.Add(new Header("my-header", new byte[] { 44 }));
            hdrs.Add(new Header("my-header-2", new byte[] { 45 }));

            int cnt = 0;
            foreach (var hdr in hdrs)
            {
                switch (cnt)
                {
                    case 0:
                        Assert.Equal(new byte[] { 42 }, hdr.GetValueBytes());
                        break;
                    case 1:
                        Assert.Equal(new byte[] { 44 }, hdr.GetValueBytes());
                        break;
                    case 2:
                        Assert.Equal(new byte[] { 45 }, hdr.GetValueBytes());
                        break;
                }
                cnt += 1;
            }

            Assert.Equal(3, cnt);
        }

    }
}
