// Copyright 20 Confluent Inc.
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
using System.Threading.Tasks;
using Xunit;


namespace Confluent.SchemaRegistry.IntegrationTests
{
    public static partial class Tests
    {
        [Theory, MemberData(nameof(SchemaRegistryParameters))]
        public static async Task UpdateCompatibilityLevel(Config config)
        {   
            var sr = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = config.Server });

            var topic = Guid.NewGuid().ToString();
            var subject = sr.ConstructValueSubjectName(topic);

            await Assert.ThrowsAsync<SchemaRegistryException>(() => sr.GetCompatibilityAsync(subject));

            var compatibility = Compatibility.Full;
            await sr.UpdateCompatibilityAsync(subject, compatibility);

            Assert.Equal(compatibility, await sr.GetCompatibilityAsync(subject));
        }
    }
}
