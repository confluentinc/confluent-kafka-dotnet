// Copyright 2026 Confluent Inc.
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
using Confluent.Kafka;
using Confluent.SchemaRegistry.Rules;
using Example;
using Xunit;

namespace Confluent.SchemaRegistry.Serdes.UnitTests
{
    public class ProtobufInlineTagTests : BaseSerializeDeserializeTests
    {
        public ProtobufInlineTagTests() : base()
        {
        }

        // A CEL_FIELD rule scoped to the "PII" tag. The only source of that tag is the
        // inline (.confluent.field_meta) option on `name` — deliberately no schema Metadata,
        // so the rule applies if and only if inline tags are honored. Regression test:
        // GetInlineTags previously read only UninterpretedOptions, which protobuf-net leaves
        // empty because it resolves the option into an extension, so inline tags on protobuf
        // fields were silently ignored and tag-scoped rules never fired.
        [Fact]
        public void InlineTagScopedRuleApplies()
        {
            string schemaStr = @"syntax = ""proto3"";
            import ""confluent/meta.proto"";

            package example;

            message PersonWithPic {
                string favorite_color = 1;
                int32 favorite_number = 2;
                string name = 3 [(.confluent.field_meta) = { tags: ""PII"" }];
                bytes picture = 4;
            }";

            var schema = new RegisteredSchema("topic-value", 1, 1, schemaStr, SchemaType.Protobuf, null);
            schema.RuleSet = new RuleSet(new List<Rule>(),
                new List<Rule>
                {
                    new Rule("suffixPII", RuleKind.Transform, RuleMode.Write, "CEL_FIELD",
                        new HashSet<string> { "PII" }, null,
                        "value + '-suffix'", null, null, false)
                }
            );
            store[schemaStr] = 1;
            subjectStore["topic-value"] = new List<RegisteredSchema> { schema };

            var config = new ProtobufSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = true
            };
            RuleRegistry ruleRegistry = new RuleRegistry();
            ruleRegistry.RegisterExecutor(new CelFieldExecutor());
            var serializer = new ProtobufSerializer<PersonWithPic>(schemaRegistryClient, config, ruleRegistry);
            var deserializer = new ProtobufDeserializer<PersonWithPic>(schemaRegistryClient, null, ruleRegistry);

            var user = new PersonWithPic
            {
                FavoriteColor = "blue",
                FavoriteNumber = 100,
                Name = "awesome"
            };

            Headers headers = new Headers();
            var bytes = serializer.SerializeAsync(user,
                new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            var result = deserializer.DeserializeAsync(bytes, false,
                new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;


            // If inline tags are honored, the tagged field is transformed.
            Assert.Equal("awesome-suffix", result.Name);
            // The untagged field must never be transformed.
            Assert.Equal("blue", result.FavoriteColor);
        }

        // Same probe, but with the dotted option form rather than the aggregate form.
        [Fact]
        public void InlineTagScopedRuleAppliesDottedForm()
        {
            string schemaStr = @"syntax = ""proto3"";
            import ""confluent/meta.proto"";

            package example;

            message PersonWithPic {
                string favorite_color = 1;
                int32 favorite_number = 2;
                string name = 3 [(confluent.field_meta).tags = ""PII""];
                bytes picture = 4;
            }";

            var schema = new RegisteredSchema("topic-value", 1, 1, schemaStr, SchemaType.Protobuf, null);
            schema.RuleSet = new RuleSet(new List<Rule>(),
                new List<Rule>
                {
                    new Rule("suffixPII", RuleKind.Transform, RuleMode.Write, "CEL_FIELD",
                        new HashSet<string> { "PII" }, null,
                        "value + '-suffix'", null, null, false)
                }
            );
            store[schemaStr] = 1;
            subjectStore["topic-value"] = new List<RegisteredSchema> { schema };

            var config = new ProtobufSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = true
            };
            RuleRegistry ruleRegistry = new RuleRegistry();
            ruleRegistry.RegisterExecutor(new CelFieldExecutor());
            var serializer = new ProtobufSerializer<PersonWithPic>(schemaRegistryClient, config, ruleRegistry);
            var deserializer = new ProtobufDeserializer<PersonWithPic>(schemaRegistryClient, null, ruleRegistry);

            var user = new PersonWithPic
            {
                FavoriteColor = "blue",
                FavoriteNumber = 100,
                Name = "awesome"
            };

            Headers headers = new Headers();
            var bytes = serializer.SerializeAsync(user,
                new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            var result = deserializer.DeserializeAsync(bytes, false,
                new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;


            Assert.Equal("awesome-suffix", result.Name);
        }
    }
}
