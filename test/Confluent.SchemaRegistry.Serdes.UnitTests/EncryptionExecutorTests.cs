// Copyright 2025 Confluent Inc.
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

using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.SchemaRegistry.Encryption;
using Xunit;

namespace Confluent.SchemaRegistry.Serdes.UnitTests
{
    public class EncryptionExecutorTests : BaseSerializeDeserializeTests
    {
        private static RuleContext NewContext(string subject)
        {
            var ruleParams = new Dictionary<string, string>
            {
                ["encrypt.kek.name"] = "kek1",
                ["encrypt.kms.type"] = "local-kms",
                ["encrypt.kms.key.id"] = "mykey"
            };
            var rule = new Rule("rule1", RuleKind.Transform, RuleMode.Write,
                EncryptionExecutor.RuleType, null, ruleParams);
            var target = new Schema("\"string\"", SchemaType.Avro);
            return new RuleContext(null, null, target, subject, null, null, false,
                RuleMode.Write, rule, 0, new List<Rule> { rule }, null);
        }

        private EncryptionExecutor NewExecutor()
        {
            var executor = new EncryptionExecutor(dekRegistryClient, clock);
            executor.Configure(new Dictionary<string, string>
            {
                ["schema.registry.url"] = "mock://",
                ["secret"] = "mysecret"
            });
            return executor;
        }

        [Fact]
        public async Task TransformThreadsContextFromSubject()
        {
            // Context-qualified subject: the context should be parsed out of the
            // subject and threaded through to the dek registry client, not dropped,
            // so the kek ends up stored under the ".myctx" context.
            using (var executor = NewExecutor())
            {
                await executor.Transform(NewContext(":.myctx:widget-value"), new byte[] { 1, 2, 3 });
            }
            Assert.True(kekStore.ContainsKey(new KekId("kek1", false, ".myctx")));

            // Unqualified subject (default context "."): the context should
            // normalize to null rather than being stored under the literal "."
            // context.
            using (var executor = NewExecutor())
            {
                await executor.Transform(NewContext("widget-value"), new byte[] { 1, 2, 3 });
            }
            Assert.True(kekStore.ContainsKey(new KekId("kek1", false, null)));

            // Explicitly-qualified default context (":.:subject"): should behave
            // identically to an unqualified subject, not be stored under the
            // literal "." context.
            using (var executor = NewExecutor())
            {
                await executor.Transform(NewContext(":.:widget-value"), new byte[] { 1, 2, 3 });
            }
            Assert.True(kekStore.ContainsKey(new KekId("kek1", false, null)));
        }
    }
}
