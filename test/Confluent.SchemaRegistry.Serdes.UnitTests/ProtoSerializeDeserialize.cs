// Copyright 2020 Confluent Inc.
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

// ConstructValueSubjectName is still used a an internal implementation detail.
#pragma warning disable CS0618

using System;
using Moq;
using Xunit;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Confluent.SchemaRegistry.Encryption;
using Example;


namespace Confluent.SchemaRegistry.Serdes.UnitTests
{
    public class ProtobufSerializeDeserializeTests : BaseSerializeDeserializeTests
    {
        public ProtobufSerializeDeserializeTests() : base()
        {
        }

        [Fact]
        public void ParseSchema()
        {
            string schema = @"syntax = ""proto3"";
            package io.confluent.kafka.serializers.protobuf.test;

            import ""ref.proto"";
            import ""confluent/meta.proto"";

            message ReferrerMessage {

                string root_id = 1 [(.confluent.field_meta) = { annotation: ""PII"" }];
                ReferencedMessage ref = 2 [(.confluent.field_meta).annotation = ""PII""];

            }";
            
            string import = @"syntax = ""proto3"";
            package io.confluent.kafka.serializers.protobuf.test;

            message ReferencedMessage {
                string ref_id = 1;
                bool is_active = 2;
            }
            ";

            IDictionary<string, string> imports = new Dictionary<string, string>();
            imports["ref.proto"] = import;

            var fds = ProtobufUtils.Parse(schema, imports);
            foreach (var file in fds.Files)
            {
                foreach (var messageType in file.MessageTypes)
                {
                    Assert.Equal("ReferrerMessage", messageType.Name);
                }
            }
        }

        [Fact]
        public void Null()
        {
            var protoSerializer = new ProtobufSerializer<UInt32Value>(schemaRegistryClient);
            var protoDeserializer = new ProtobufDeserializer<UInt32Value>(schemaRegistryClient);

            var bytes = protoSerializer.SerializeAsync(null, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            Assert.Null(bytes);
            Assert.Null(protoDeserializer.DeserializeAsync(bytes, true, new SerializationContext(MessageComponentType.Value, testTopic)).Result);
        }

        [Fact]
        public void UInt32SerDe()
        {
            var protoSerializer = new ProtobufSerializer<UInt32Value>(schemaRegistryClient);
            var protoDeserializer = new ProtobufDeserializer<UInt32Value>();

            var v = new UInt32Value { Value = 1234 };
            var bytes = protoSerializer.SerializeAsync(v, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            Assert.Equal(v.Value, protoDeserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic)).Result.Value);
        }

        [Fact]
        public void CELCondition()
        {
            string schemaStr = @"syntax = ""proto3"";
            import ""confluent/meta.proto"";

            package example;

            message Person {
                string favorite_color = 1;
                int32 favorite_number = 2;
                string name = 3;
            }";
            var schema = new RegisteredSchema("topic-value", 1, 1, schemaStr, SchemaType.Protobuf, null);
            schema.RuleSet = new RuleSet(new List<Rule>(),
                new List<Rule>
                {
                    new Rule("testCEL", RuleKind.Condition, RuleMode.Write, "CEL", null, null, 
                        "message.name == 'awesome'", null, null, false)
                }
            );
            store[schemaStr] = 1;
            subjectStore["topic-value"] = new List<RegisteredSchema> { schema }; 
            var config = new ProtobufSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = true
            };
            var serializer = new ProtobufSerializer<Person>(schemaRegistryClient, config);
            var deserializer = new ProtobufDeserializer<Person>(schemaRegistryClient, null);

            var user = new Person
            {
                FavoriteColor = "blue",
                FavoriteNumber = 100,
                Name = "awesome"
            };

            Headers headers = new Headers();
            var bytes = serializer.SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            var result = deserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;

            Assert.Equal("awesome", result.Name);
            Assert.Equal(user.FavoriteColor, result.FavoriteColor);
            Assert.Equal(user.FavoriteNumber, result.FavoriteNumber);
        }

        [Fact]
        public void CELConditionFail()
        {
            string schemaStr = @"syntax = ""proto3"";
            import ""confluent/meta.proto"";

            package example;

            message Person {
                string favorite_color = 1;
                int32 favorite_number = 2;
                string name = 3;
            }";
            var schema = new RegisteredSchema("topic-value", 1, 1, schemaStr, SchemaType.Protobuf, null);
            schema.RuleSet = new RuleSet(new List<Rule>(),
                new List<Rule>
                {
                    new Rule("testCEL", RuleKind.Condition, RuleMode.Write, "CEL", null, null, 
                        "message.name != 'awesome'", null, null, false)
                }
            );
            store[schemaStr] = 1;
            subjectStore["topic-value"] = new List<RegisteredSchema> { schema }; 
            var config = new ProtobufSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = true
            };
            var serializer = new ProtobufSerializer<Person>(schemaRegistryClient, config);

            var user = new Person
            {
                FavoriteColor = "blue",
                FavoriteNumber = 100,
                Name = "awesome"
            };

            Headers headers = new Headers();
            Assert.Throws<AggregateException>(() => serializer.SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result);
        }

        [Fact]
        public void CELFieldTransform()
        {
            string schemaStr = @"syntax = ""proto3"";
            import ""confluent/meta.proto"";

            package example;

            message Person {
                string favorite_color = 1;
                int32 favorite_number = 2;
                string name = 3;
            }";
            var schema = new RegisteredSchema("topic-value", 1, 1, schemaStr, SchemaType.Protobuf, null);
            schema.RuleSet = new RuleSet(new List<Rule>(),
                new List<Rule>
                {
                    new Rule("testCEL", RuleKind.Transform, RuleMode.Write, "CEL_FIELD", null, null, 
                        "typeName == 'STRING' ; value + '-suffix'", null, null, false)
                }
            );
            store[schemaStr] = 1;
            subjectStore["topic-value"] = new List<RegisteredSchema> { schema }; 
            var config = new ProtobufSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = true
            };
            var serializer = new ProtobufSerializer<Person>(schemaRegistryClient, config);
            var deserializer = new ProtobufDeserializer<Person>(schemaRegistryClient, null);

            var user = new Person
            {
                FavoriteColor = "blue",
                FavoriteNumber = 100,
                Name = "awesome"
            };

            Headers headers = new Headers();
            var bytes = serializer.SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            var result = deserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;

            Assert.Equal("awesome-suffix", result.Name);
            Assert.Equal("blue-suffix", result.FavoriteColor);
            Assert.Equal(user.FavoriteNumber, result.FavoriteNumber);
        }

        [Fact]
        public void CELFieldCondition()
        {
            string schemaStr = @"syntax = ""proto3"";
            import ""confluent/meta.proto"";

            package example;

            message Person {
                string favorite_color = 1;
                int32 favorite_number = 2;
                string name = 3;
            }";
            var schema = new RegisteredSchema("topic-value", 1, 1, schemaStr, SchemaType.Protobuf, null);
            schema.RuleSet = new RuleSet(new List<Rule>(),
                new List<Rule>
                {
                    new Rule("testCEL", RuleKind.Condition, RuleMode.Write, "CEL_FIELD", null, null, 
                        "name == 'name' ; value == 'awesome'", null, null, false)
                }
            );
            store[schemaStr] = 1;
            subjectStore["topic-value"] = new List<RegisteredSchema> { schema }; 
            var config = new ProtobufSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = true
            };
            var serializer = new ProtobufSerializer<Person>(schemaRegistryClient, config);
            var deserializer = new ProtobufDeserializer<Person>(schemaRegistryClient, null);

            var user = new Person
            {
                FavoriteColor = "blue",
                FavoriteNumber = 100,
                Name = "awesome"
            };

            Headers headers = new Headers();
            var bytes = serializer.SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            var result = deserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;

            Assert.Equal("awesome", result.Name);
            Assert.Equal(user.FavoriteColor, result.FavoriteColor);
            Assert.Equal(user.FavoriteNumber, result.FavoriteNumber);
        }

        [Fact]
        public void CELFieldConditionFail()
        {
            string schemaStr = @"syntax = ""proto3"";
            import ""confluent/meta.proto"";

            package example;

            message Person {
                string favorite_color = 1;
                int32 favorite_number = 2;
                string name = 3;
            }";
            var schema = new RegisteredSchema("topic-value", 1, 1, schemaStr, SchemaType.Protobuf, null);
            schema.RuleSet = new RuleSet(new List<Rule>(),
                new List<Rule>
                {
                    new Rule("testCEL", RuleKind.Condition, RuleMode.Write, "CEL_FIELD", null, null, 
                        "name == 'name' ; value != 'awesome'", null, null, false)
                }
            );
            store[schemaStr] = 1;
            subjectStore["topic-value"] = new List<RegisteredSchema> { schema }; 
            var config = new ProtobufSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = true
            };
            var serializer = new ProtobufSerializer<Person>(schemaRegistryClient, config);

            var user = new Person
            {
                FavoriteColor = "blue",
                FavoriteNumber = 100,
                Name = "awesome"
            };

            Headers headers = new Headers();
            Assert.Throws<AggregateException>(() => serializer.SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result);
        }

        [Fact]
        public void FieldEncryption()
        {
            string schemaStr = @"syntax = ""proto3"";
            import ""confluent/meta.proto"";

            package example;

            message Person {
                string favorite_color = 1;
                int32 favorite_number = 2;
                string name = 3 [(.confluent.field_meta) = { tags: ""PII"" }];
            }";
            
            var schema = new RegisteredSchema("topic-value", 1, 1, schemaStr, SchemaType.Protobuf, null);
            schema.Metadata = new Metadata(new Dictionary<string, ISet<string>>
                {
                    ["example.Person.name"] = new HashSet<string> { "PII" }

                }, new Dictionary<string, string>(), new HashSet<string>()
            );
            schema.RuleSet = new RuleSet(new List<Rule>(),
                new List<Rule>
                {
                    new Rule("encryptPII", RuleKind.Transform, RuleMode.WriteRead, "ENCRYPT", new HashSet<string>
                    {
                        "PII"
                    }, new Dictionary<string, string>
                    {
                        ["encrypt.kek.name"] = "kek1",
                        ["encrypt.kms.type"] = "local-kms",
                        ["encrypt.kms.key.id"] = "mykey"
                    })
                }
            );
            store[schemaStr] = 1;
            subjectStore["topic-value"] = new List<RegisteredSchema> { schema }; 
            var config = new ProtobufSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = true
            };
            config.Set("rules.secret", "mysecret");
            IRuleExecutor ruleExecutor = new FieldEncryptionExecutor(dekRegistryClient);
            var serializer = new ProtobufSerializer<Person>(schemaRegistryClient, config, new List<IRuleExecutor>{ ruleExecutor});
            var deserializer = new ProtobufDeserializer<Person>(schemaRegistryClient, null, new List<IRuleExecutor>{ ruleExecutor});

            var user = new Person
            {
                FavoriteColor = "blue",
                FavoriteNumber = 100,
                Name = "awesome"
            };

            Headers headers = new Headers();
            var bytes = serializer.SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            var result = deserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;

            // The user name has been modified
            Assert.Equal("awesome", result.Name);
            Assert.Equal(user.FavoriteColor, result.FavoriteColor);
            Assert.Equal(user.FavoriteNumber, result.FavoriteNumber);
        }

    }
}
