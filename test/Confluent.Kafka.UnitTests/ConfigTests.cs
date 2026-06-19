// Copyright 2026 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System.Collections.Generic;
using Xunit;

namespace Confluent.Kafka.UnitTests
{
    public class ConfigTests
    {
        // ---- Config.Snapshot ----

        [Fact]
        public void Snapshot_NullEnumerable_ReturnsEmpty()
        {
            var snap = Config.Snapshot(null);
            Assert.Empty(snap);
        }

        [Fact]
        public void Snapshot_DistinctKeys_AllPreserved()
        {
            var entries = new[]
            {
                new KeyValuePair<string, string>("a", "1"),
                new KeyValuePair<string, string>("b", "2"),
            };
            var snap = Config.Snapshot(entries);
            Assert.Equal(2, snap.Count);
            Assert.Equal("1", snap["a"]);
            Assert.Equal("2", snap["b"]);
        }

        [Fact]
        public void Snapshot_DuplicateKeys_LastWins()
        {
            var entries = new[]
            {
                new KeyValuePair<string, string>("k", "v1"),
                new KeyValuePair<string, string>("k", "v2"),
            };
            var snap = Config.Snapshot(entries);
            Assert.Single(snap);
            Assert.Equal("v2", snap["k"]);
        }
    }
}
