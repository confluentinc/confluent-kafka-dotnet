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

// Disable obsolete warnings. ConstructValueSubjectName is still used a an internal implementation detail.
#pragma warning disable CS0618

using Xunit;
using System.Collections.Generic;
using Avro.Specific;
using Confluent.Kafka;
using Confluent.Kafka.Examples.AvroSpecific;
using System;
using System.Linq;
using Avro;
using Avro.Generic;
using Confluent.SchemaRegistry.Encryption;


namespace Confluent.SchemaRegistry.Serdes.UnitTests
{
    public class SerializeDeserializeTests : BaseSerializeDeserializeTests
    {
        public SerializeDeserializeTests() : base()
        {
        }

        [Fact]
        public void IntSerDe()
        {
            var avroSerializer = new AvroSerializer<int>(schemaRegistryClient);
            var avroDeserializer = new AvroDeserializer<int>(schemaRegistryClient);
            byte[] bytes;
            bytes = avroSerializer.SerializeAsync(1234, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            Assert.Equal(1234, avroDeserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic)).Result);
        }

        [Fact]
        public void LongSerDe()
        {
            var avroSerializer = new AvroSerializer<long>(schemaRegistryClient);
            var avroDeserializer = new AvroDeserializer<long>(schemaRegistryClient);
            byte[] bytes;
            bytes = avroSerializer.SerializeAsync(123, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            Assert.Equal(123, avroDeserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic)).Result);
        }

        [Fact]
        public void BoolSerDe()
        {
            var avroSerializer = new AvroSerializer<bool>(schemaRegistryClient);
            var avroDeserializer = new AvroDeserializer<bool>(schemaRegistryClient);
            byte[] bytes;
            bytes = avroSerializer.SerializeAsync(true, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            Assert.Equal(true, avroDeserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic)).Result);
        }

        [Fact]
        public void StringSerDe()
        {
            var avroSerializer = new AvroSerializer<string>(schemaRegistryClient);
            var avroDeserializer = new AvroDeserializer<string>(schemaRegistryClient);
            byte[] bytes;
            bytes = avroSerializer.SerializeAsync("abc", new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            Assert.Equal("abc", avroDeserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic)).Result);
        }

        [Fact]
        public void DoubleSerDe()
        {
            var avroSerializer = new AvroSerializer<double>(schemaRegistryClient);
            var avroDeserializer = new AvroDeserializer<double>(schemaRegistryClient);
            byte[] bytes;
            bytes = avroSerializer.SerializeAsync(123d, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            Assert.Equal(123d, avroDeserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic)).Result);
        }

        [Fact]
        public void FloatSerDe()
        {
            var avroSerializer = new AvroSerializer<float>(schemaRegistryClient);
            var avroDeserializer = new AvroDeserializer<float>(schemaRegistryClient);
            byte[] bytes;
            bytes = avroSerializer.SerializeAsync(123f, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            Assert.Equal(123f, avroDeserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic)).Result);
        }

        [Fact]
        public void BytesSerDe()
        {
            var avroSerializer = new AvroSerializer<byte[]>(schemaRegistryClient);
            var avroDeserializer = new AvroDeserializer<byte[]>(schemaRegistryClient);
            byte[] bytes;
            bytes = avroSerializer.SerializeAsync(new byte[] { 2, 3, 4 }, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            Assert.Equal(new byte[] { 2, 3, 4 }, avroDeserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic)).Result);
        }

        [Fact]
        public void NullSerDe()
        {
            var avroSerializer = new AvroSerializer<Null>(schemaRegistryClient);
            var avroDeserializer = new AvroDeserializer<Null>(schemaRegistryClient);
            byte[] bytes;
            bytes = avroSerializer.SerializeAsync(null, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            Assert.Equal(null, avroDeserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic)).Result);
        }

        [Fact]
        public void ISpecificRecord()
        {
            var serializer = new AvroSerializer<User>(schemaRegistryClient);
            var deserializer = new AvroDeserializer<User>(schemaRegistryClient);

            var user = new User
            {
                favorite_color = "blue",
                favorite_number = 100,
                name = "awesome"
            };

            var bytes = serializer.SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            var result = deserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic)).Result;

            Assert.Equal(user.name, result.name);
            Assert.Equal(user.favorite_color, result.favorite_color);
            Assert.Equal(user.favorite_number, result.favorite_number);

            // serialize second object
            user = new User
            {
                favorite_color = "red",
                favorite_number = 100,
                name = "awesome"
            };

            bytes = serializer.SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            result = deserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic)).Result;

            Assert.Equal(user.name, result.name);
            Assert.Equal(user.favorite_color, result.favorite_color);
            Assert.Equal(user.favorite_number, result.favorite_number);
        }

        [Fact]
        public void ISpecificRecordRecordNameStrategy()
        {
            var serializerConfig = new AvroSerializerConfig
            {
                SubjectNameStrategy = SubjectNameStrategy.Record
            };
            var serializer = new AvroSerializer<User>(schemaRegistryClient, serializerConfig);
            var deserializerConfig = new AvroDeserializerConfig
            {
                SubjectNameStrategy = SubjectNameStrategy.Record
            };
            var deserializer = new AvroDeserializer<User>(schemaRegistryClient, deserializerConfig);

            var user = new User
            {
                favorite_color = "blue",
                favorite_number = 100,
                name = "awesome"
            };

            var bytes = serializer.SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            var result = deserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic)).Result;

            Assert.Equal(user.name, result.name);
            Assert.Equal(user.favorite_color, result.favorite_color);
            Assert.Equal(user.favorite_number, result.favorite_number);
            
            // serialize second object
            user = new User
            {
                favorite_color = "red",
                favorite_number = 100,
                name = "awesome"
            };

            bytes = serializer.SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            result = deserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic)).Result;

            Assert.Equal(user.name, result.name);
            Assert.Equal(user.favorite_color, result.favorite_color);
            Assert.Equal(user.favorite_number, result.favorite_number);
        }

        [Fact]
        public void ISpecificRecordCELCondition()
        {
            var schemaStr = User._SCHEMA.ToString();
            var schema = new RegisteredSchema("topic-value", 1, 1, schemaStr, SchemaType.Avro, null);
            schema.RuleSet = new RuleSet(new List<Rule>(),
                new List<Rule>
                {
                    new Rule("testCEL", RuleKind.Condition, RuleMode.Write, "CEL", null, null, 
                        "message.name == 'awesome'", null, null, false)
                }
            );
            store[schemaStr] = 1;
            subjectStore["topic-value"] = new List<RegisteredSchema> { schema }; 
            var config = new AvroSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = true
            };
            var serializer = new AvroSerializer<User>(schemaRegistryClient, config);
            var deserializer = new AvroDeserializer<User>(schemaRegistryClient, null);

            var user = new User
            {
                favorite_color = "blue",
                favorite_number = 100,
                name = "awesome"
            };

            Headers headers = new Headers();
            var bytes = serializer.SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            var result = deserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;

            Assert.Equal("awesome", result.name);
            Assert.Equal(user.favorite_color, result.favorite_color);
            Assert.Equal(user.favorite_number, result.favorite_number);
        }

        [Fact]
        public void ISpecificRecordCELConditionFail()
        {
            var schemaStr = User._SCHEMA.ToString();
            var schema = new RegisteredSchema("topic-value", 1, 1, schemaStr, SchemaType.Avro, null);
            schema.RuleSet = new RuleSet(new List<Rule>(),
                new List<Rule>
                {
                    new Rule("testCEL", RuleKind.Condition, RuleMode.Write, "CEL", null, null, 
                        "message.name != 'awesome'", null, null, false)
                }
            );
            store[schemaStr] = 1;
            subjectStore["topic-value"] = new List<RegisteredSchema> { schema }; 
            var config = new AvroSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = true
            };
            var serializer = new AvroSerializer<User>(schemaRegistryClient, config);

            var user = new User
            {
                favorite_color = "blue",
                favorite_number = 100,
                name = "awesome"
            };

            Headers headers = new Headers();
            Assert.Throws<AggregateException>(() => serializer.SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result);
        }

        [Fact]
        public void ISpecificRecordCELFieldTransform()
        {
            var schemaStr = User._SCHEMA.ToString();
            var schema = new RegisteredSchema("topic-value", 1, 1, schemaStr, SchemaType.Avro, null);
            schema.RuleSet = new RuleSet(new List<Rule>(),
                new List<Rule>
                {
                    new Rule("testCEL", RuleKind.Transform, RuleMode.Write, "CEL_FIELD", null, null, 
                        "typeName == 'STRING' ; value + '-suffix'", null, null, false)
                }
            );
            store[schemaStr] = 1;
            subjectStore["topic-value"] = new List<RegisteredSchema> { schema }; 
            var config = new AvroSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = true
            };
            var serializer = new AvroSerializer<User>(schemaRegistryClient, config);
            var deserializer = new AvroDeserializer<User>(schemaRegistryClient, null);

            var user = new User
            {
                favorite_color = "blue",
                favorite_number = 100,
                name = "awesome"
            };

            Headers headers = new Headers();
            var bytes = serializer.SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            var result = deserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;

            Assert.Equal("awesome-suffix", result.name);
            Assert.Equal("blue-suffix", result.favorite_color);
            Assert.Equal(user.favorite_number, result.favorite_number);
        }

        [Fact]
        public void ISpecificRecordCELFieldCondition()
        {
            var schemaStr = User._SCHEMA.ToString();
            var schema = new RegisteredSchema("topic-value", 1, 1, schemaStr, SchemaType.Avro, null);
            schema.RuleSet = new RuleSet(new List<Rule>(),
                new List<Rule>
                {
                    new Rule("testCEL", RuleKind.Condition, RuleMode.Write, "CEL_FIELD", null, null, 
                        "name == 'name' ; value == 'awesome'", null, null, false)
                }
            );
            store[schemaStr] = 1;
            subjectStore["topic-value"] = new List<RegisteredSchema> { schema }; 
            var config = new AvroSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = true
            };
            var serializer = new AvroSerializer<User>(schemaRegistryClient, config);
            var deserializer = new AvroDeserializer<User>(schemaRegistryClient, null);

            var user = new User
            {
                favorite_color = "blue",
                favorite_number = 100,
                name = "awesome"
            };

            Headers headers = new Headers();
            var bytes = serializer.SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            var result = deserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;

            Assert.Equal("awesome", result.name);
            Assert.Equal("blue", result.favorite_color);
            Assert.Equal(user.favorite_number, result.favorite_number);
        }

        [Fact]
        public void ISpecificRecordCELFieldConditionFail()
        {
            var schemaStr = User._SCHEMA.ToString();
            var schema = new RegisteredSchema("topic-value", 1, 1, schemaStr, SchemaType.Avro, null);
            schema.RuleSet = new RuleSet(new List<Rule>(),
                new List<Rule>
                {
                    new Rule("testCEL", RuleKind.Condition, RuleMode.Write, "CEL_FIELD", null, null, 
                        "name == 'name' ; value != 'awesome'", null, null, false)
                }
            );
            store[schemaStr] = 1;
            subjectStore["topic-value"] = new List<RegisteredSchema> { schema }; 
            var config = new AvroSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = true
            };
            var serializer = new AvroSerializer<User>(schemaRegistryClient, config);

            var user = new User
            {
                favorite_color = "blue",
                favorite_number = 100,
                name = "awesome"
            };

            Headers headers = new Headers();
            Assert.Throws<AggregateException>(() => serializer.SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result);
        }

        [Fact]
        public void ISpecificRecordFieldEncryption()
        {
            var schemaStr = "{\"type\":\"record\",\"name\":\"UserWithPic\",\"namespace\":\"Confluent.Kafka.Examples.AvroSpecific" +
                    "\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\"," +
                    "\"type\":[\"int\",\"null\"]},{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"]}," +
                    "{\"name\":\"picture\",\"type\":[\"null\",\"bytes\"],\"default\":null}]}";

            var schema = new RegisteredSchema("topic-value", 1, 1, schemaStr, SchemaType.Avro, null);
            schema.Metadata = new Metadata(new Dictionary<string, ISet<string>>
                {
                    ["Confluent.Kafka.Examples.AvroSpecific.UserWithPic.name"] = new HashSet<string> { "PII" },
                    ["Confluent.Kafka.Examples.AvroSpecific.UserWithPic.picture"] = new HashSet<string> { "PII" }

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
            var config = new AvroSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = true
            };
            config.Set("rules.secret", "mysecret");
            RuleRegistry ruleRegistry = new RuleRegistry();
            IRuleExecutor ruleExecutor = new FieldEncryptionExecutor(dekRegistryClient, clock);
            ruleRegistry.RegisterExecutor(ruleExecutor);
            var serializer = new AvroSerializer<UserWithPic>(schemaRegistryClient, config, ruleRegistry);
            var deserializer = new AvroDeserializer<UserWithPic>(schemaRegistryClient, null, ruleRegistry);

            var pic = new byte[] { 1, 2, 3 };
            var user = new UserWithPic()
            {
                favorite_color = "blue",
                favorite_number = 100,
                name = "awesome",
                picture = pic
            };

            Headers headers = new Headers();
            var bytes = serializer.SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            var result = deserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;

            // The user name has been modified
            Assert.Equal("awesome", result.name);
            Assert.Equal(user.favorite_color, result.favorite_color);
            Assert.Equal(user.favorite_number, result.favorite_number);
            Assert.True(pic.SequenceEqual(result.picture));
        }

        [Fact]
        public void ISpecificRecordFieldEncryptionDekRotation()
        {
            var schemaStr =
                "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"Confluent.Kafka.Examples.AvroSpecific" +
                "\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\"," +
                "\"type\":[\"int\",\"null\"]},{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"]}]}";

            var schema = new RegisteredSchema("topic-value", 1, 1, schemaStr, SchemaType.Avro, null);
            schema.Metadata = new Metadata(new Dictionary<string, ISet<string>>
                {
                    ["Confluent.Kafka.Examples.AvroSpecific.User.name"] = new HashSet<string> { "PII" },

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
                        ["encrypt.kms.key.id"] = "mykey",
                        ["encrypt.dek.expiry.days"] = "1"
                    })
                }
            );
            store[schemaStr] = 1;
            subjectStore["topic-value"] = new List<RegisteredSchema> { schema }; 
            var config = new AvroSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = true
            };
            config.Set("rules.secret", "mysecret");
            RuleRegistry ruleRegistry = new RuleRegistry();
            IRuleExecutor ruleExecutor = new FieldEncryptionExecutor(dekRegistryClient, clock);
            ruleRegistry.RegisterExecutor(ruleExecutor);
            var serializer = new AvroSerializer<User>(schemaRegistryClient, config, ruleRegistry);
            var deserializer = new AvroDeserializer<User>(schemaRegistryClient, null, ruleRegistry);

            var user = new User()
            {
                favorite_color = "blue",
                favorite_number = 100,
                name = "awesome"
            };

            Headers headers = new Headers();
            var bytes = serializer.SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            var result = deserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;

            // The user name has been modified
            Assert.Equal("awesome", result.name);
            Assert.Equal(user.favorite_color, result.favorite_color);
            Assert.Equal(user.favorite_number, result.favorite_number);

            RegisteredDek dek = dekRegistryClient.GetDekVersionAsync(
                "kek1", "topic-value", -1, DekFormat.AES256_GCM, false).Result;
            Assert.Equal(1, dek.Version);

            // Advance 2 days
            now += 2 * 24 * 60 * 60 * 1000;

            user = new User()
            {
                favorite_color = "blue",
                favorite_number = 100,
                name = "awesome"
            };

            bytes = serializer.SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            result = deserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;

            // The user name has been modified
            Assert.Equal("awesome", result.name);
            Assert.Equal(user.favorite_color, result.favorite_color);
            Assert.Equal(user.favorite_number, result.favorite_number);

            dek = dekRegistryClient.GetDekVersionAsync(
                "kek1", "topic-value", -1, DekFormat.AES256_GCM, false).Result;
            Assert.Equal(2, dek.Version);

            // Advance 2 days
            now += 2 * 24 * 60 * 60 * 1000;

            user = new User()
            {
                favorite_color = "blue",
                favorite_number = 100,
                name = "awesome"
            };

            bytes = serializer.SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            result = deserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;

            // The user name has been modified
            Assert.Equal("awesome", result.name);
            Assert.Equal(user.favorite_color, result.favorite_color);
            Assert.Equal(user.favorite_number, result.favorite_number);

            dek = dekRegistryClient.GetDekVersionAsync(
                "kek1", "topic-value", -1, DekFormat.AES256_GCM, false).Result;
            Assert.Equal(3, dek.Version);
        }

        [Fact]
        public void ISpecificRecordJSONataFullyCompatible()
        {
            var rule1To2 = "$merge([$sift($, function($v, $k) {$k != 'name'}), {'full_name': $.'name'}])";
            var rule2To1 = "$merge([$sift($, function($v, $k) {$k != 'full_name'}), {'name': $.'full_name'}])";
            var rule2To3 = "$merge([$sift($, function($v, $k) {$k != 'full_name'}), {'title': $.'full_name'}])";
            var rule3To2 = "$merge([$sift($, function($v, $k) {$k != 'title'}), {'full_name': $.'title'}])";

            var schemaStr = User._SCHEMA.ToString();
            var schema = new RegisteredSchema("topic-value", 1, 1, schemaStr, SchemaType.Avro, null);
            schema.Metadata = new Metadata(null, new Dictionary<string, string>
                {
                    { "application.version", "1"}
                    
                }, new HashSet<string>()
            );
            store[schemaStr] = 1;
            var config1 = new AvroSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = false,
                UseLatestWithMetadata = new Dictionary<string, string>{ { "application.version", "1"} }
            };
            var deserConfig1 = new AvroDeserializerConfig
            {
                UseLatestVersion = false,
                UseLatestWithMetadata = new Dictionary<string, string>{ { "application.version", "1"} }
            };
            var serializer1 = new AvroSerializer<User>(schemaRegistryClient, config1);
            var deserializer1 = new AvroDeserializer<User>(schemaRegistryClient, deserConfig1);

            var user = new User
            {
                favorite_color = "blue",
                favorite_number = 100,
                name = "awesome"
            };

            var newSchemaStr = NewUser._SCHEMA.ToString();
            var newSchema = new RegisteredSchema("topic-value", 2, 2, newSchemaStr, SchemaType.Avro, null);
            newSchema.Metadata = new Metadata(null, new Dictionary<string, string>
                {
                    { "application.version", "2"}
                    
                }, new HashSet<string>()
            );
            newSchema.RuleSet = new RuleSet(
                new List<Rule>
                {
                    new Rule("myRule1", RuleKind.Transform, RuleMode.Upgrade, "JSONATA", null, 
                        null, rule1To2, null, null, false),
                    new Rule("myRule2", RuleKind.Transform, RuleMode.Downgrade, "JSONATA", null, 
                        null, rule2To1, null, null, false)
                }, new List<Rule>()
            );
            var config2 = new AvroSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = false,
                UseLatestWithMetadata = new Dictionary<string, string>{ { "application.version", "2"} }
            };
            var deserConfig2 = new AvroDeserializerConfig
            {
                UseLatestVersion = false,
                UseLatestWithMetadata = new Dictionary<string, string>{ { "application.version", "2"} }
            };
            var serializer2 = new AvroSerializer<NewUser>(schemaRegistryClient, config2);
            var deserializer2 = new AvroDeserializer<NewUser>(schemaRegistryClient, deserConfig2);

            var newUser = new NewUser
            {
                favorite_color = "blue",
                favorite_number = 100,
                full_name = "awesome"
            };

            var newerSchemaStr = NewerUser._SCHEMA.ToString();
            var newerSchema = new RegisteredSchema("topic-value", 3, 3, newerSchemaStr, SchemaType.Avro, null);
            newerSchema.Metadata = new Metadata(null, new Dictionary<string, string>
                {
                    { "application.version", "3"}
                    
                }, new HashSet<string>()
            );
            newerSchema.RuleSet = new RuleSet(
                new List<Rule>
                {
                    new Rule("myRule1", RuleKind.Transform, RuleMode.Upgrade, "JSONATA", null, 
                        null, rule2To3, null, null, false),
                    new Rule("myRule2", RuleKind.Transform, RuleMode.Downgrade, "JSONATA", null, 
                        null, rule3To2, null, null, false)
                }, new List<Rule>()
            );
            var config3 = new AvroSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = false,
                UseLatestWithMetadata = new Dictionary<string, string>{ { "application.version", "3"} }
            };
            var deserConfig3 = new AvroDeserializerConfig
            {
                UseLatestVersion = false,
                UseLatestWithMetadata = new Dictionary<string, string>{ { "application.version", "3"} }
            };
            var serializer3 = new AvroSerializer<NewerUser>(schemaRegistryClient, config3);
            var deserializer3 = new AvroDeserializer<NewerUser>(schemaRegistryClient, deserConfig3);

            var newerUser = new NewerUser
            {
                favorite_color = "blue",
                favorite_number = 100,
                title = "awesome"
            };
            
            store[schemaStr] = 1;
            store[newSchemaStr] = 2;
            store[newerSchemaStr] = 3;
            subjectStore["topic-value"] = new List<RegisteredSchema> { schema, newSchema, newerSchema }; 

            Headers headers = new Headers();
            var bytes = serializer1.SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            ISpecificRecordDeserializeAllVersions(deserializer1, deserializer2, deserializer3, bytes, headers, user);
            
            bytes = serializer2.SerializeAsync(newUser, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            ISpecificRecordDeserializeAllVersions(deserializer1, deserializer2, deserializer3, bytes, headers, user);
            
            bytes = serializer3.SerializeAsync(newerUser, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            ISpecificRecordDeserializeAllVersions(deserializer1, deserializer2, deserializer3, bytes, headers, user);
        }

        private void ISpecificRecordDeserializeAllVersions(AvroDeserializer<User> deserializer1, 
            AvroDeserializer<NewUser> deserializer2, AvroDeserializer<NewerUser> deserializer3, 
            byte[] bytes, Headers headers, User user)
        {
            var result1 = deserializer1.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            var result2 = deserializer2.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            var result3 = deserializer3.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;

            Assert.Equal("awesome", result1.name);
            Assert.Equal(user.favorite_color, result1.favorite_color);
            Assert.Equal(user.favorite_number, result1.favorite_number);

            Assert.Equal("awesome", result2.full_name);
            Assert.Equal(user.favorite_color, result2.favorite_color);
            Assert.Equal(user.favorite_number, result2.favorite_number);

            Assert.Equal("awesome", result3.title);
            Assert.Equal(user.favorite_color, result3.favorite_color);
            Assert.Equal(user.favorite_number, result3.favorite_number);
        }

        [Fact]
        public void GenericRecord()
        {
            var serializer = new AvroSerializer<GenericRecord>(schemaRegistryClient, null);
            var deserializer = new AvroDeserializer<GenericRecord>(schemaRegistryClient, null);

            var user = new GenericRecord((RecordSchema) User._SCHEMA);
            user.Add("name", "awesome");
            user.Add("favorite_number", 100);
            user.Add("favorite_color", "blue");

            Headers headers = new Headers();
            var bytes = serializer.SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            var result = deserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;

            Assert.Equal(user["name"], result["name"]);
            Assert.Equal(user["favorite_color"], result["favorite_color"]);
            Assert.Equal(user["favorite_number"], result["favorite_number"]);

            // serialize second object
            user = new GenericRecord((RecordSchema) User._SCHEMA);
            user.Add("name", "cool");
            user.Add("favorite_number", 100);
            user.Add("favorite_color", "red");

            bytes = serializer.SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            result = deserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;

            Assert.Equal(user["name"], result["name"]);
            Assert.Equal(user["favorite_color"], result["favorite_color"]);
            Assert.Equal(user["favorite_number"], result["favorite_number"]);
        }

        [Fact]
        public void GenericRecordSchemaEvolution()
        {
            var schemaStr1 = "{\n"
                             + "  \"name\": \"SchemaEvolution\",\n"
                             + "  \"type\": \"record\",\n"
                             + "  \"fields\": [\n"
                             + "    {\n"
                             + "      \"name\": \"fieldToDelete\",\n"
                             + "      \"type\": \"string\"\n"
                             + "    }\n"
                             + "  ]\n"
                             + "}";
            var schemaStr2 = "{\n"
                             + "  \"name\": \"SchemaEvolution\",\n"
                             + "  \"type\": \"record\",\n"
                             + "  \"fields\": [\n"
                             + "    {\n"
                             + "      \"name\": \"newOptionalField\",\n"
                             + "      \"type\": [\"string\", \"null\"],\n"
                             + "      \"default\": \"optional\"\n"
                             + "    }\n"
                             + "  ]\n"
                             + "}";

            var schema1 = new RegisteredSchema("topic-value", 1, 1, schemaStr1, SchemaType.Avro, null);
            var schema2 = new RegisteredSchema("topic-value", 2, 2, schemaStr2, SchemaType.Avro, null);
            store[schemaStr1] = 1;
            subjectStore["topic-value"] = new List<RegisteredSchema> { schema1 };
            var serConfig = new AvroSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = true
            };
            var serializer = new AvroSerializer<GenericRecord>(schemaRegistryClient, serConfig);
            var deserConfig = new AvroDeserializerConfig
            {
                UseLatestVersion = true
            };
            var deserializer = new AvroDeserializer<GenericRecord>(schemaRegistryClient, deserConfig);

            var obj = new GenericRecord((RecordSchema) Avro.Schema.Parse(schemaStr1));
            obj.Add("fieldToDelete", "bye");

            Headers headers = new Headers();
            var bytes = serializer.SerializeAsync(obj, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;

            store[schemaStr2] = 2;
            subjectStore["topic-value"] = new List<RegisteredSchema> { schema1, schema2 };
            var result = deserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;

            Assert.Equal(result["newOptionalField"], "optional");
        }

        [Fact]
        public void GenericRecordCELCondition()
        {
            var schemaStr = User._SCHEMA.ToString();
            var schema = new RegisteredSchema("topic-value", 1, 1, schemaStr, SchemaType.Avro, null);
            schema.RuleSet = new RuleSet(new List<Rule>(),
                new List<Rule>
                {
                    new Rule("testCEL", RuleKind.Condition, RuleMode.Write, "CEL", null, null,
                        "message.name == 'awesome'", null, null, false)
                }
            );
            store[schemaStr] = 1;
            subjectStore["topic-value"] = new List<RegisteredSchema> { schema };
            var config = new AvroSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = true
            };
            var serializer = new AvroSerializer<GenericRecord>(schemaRegistryClient, config);
            var deserializer = new AvroDeserializer<GenericRecord>(schemaRegistryClient, null);

            var user = new GenericRecord((RecordSchema) User._SCHEMA);
            user.Add("name", "awesome");
            user.Add("favorite_number", 100);
            user.Add("favorite_color", "blue");

            Headers headers = new Headers();
            var bytes = serializer.SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            var result = deserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;

            Assert.Equal("awesome", result["name"]);
            Assert.Equal(user["favorite_color"], result["favorite_color"]);
            Assert.Equal(user["favorite_number"], result["favorite_number"]);
        }

        [Fact]
        public void GenericRecordCELConditionLogicalType()
        {
            var uuid = "550e8400-e29b-41d4-a716-446655440000";
            var schemaStr = "{\"type\":\"record\",\"name\":\"UserWithPic\",\"namespace\":\"Confluent.Kafka.Examples.AvroSpecific" +
                    "\",\"fields\":[{\"name\":\"name\",\"type\":{\"type\":\"string\",\"logicalType\":\"uuid\"}},{\"name\":\"favorite_number\"," +
                    "\"type\":[\"int\",\"null\"]},{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"]}," +
                    "{\"name\":\"picture\",\"type\":[\"null\",\"bytes\"],\"default\":null}]}";

            var schema = new RegisteredSchema("topic-value", 1, 1, schemaStr, SchemaType.Avro, null);
            schema.RuleSet = new RuleSet(new List<Rule>(),
                new List<Rule>
                {
                    new Rule("testCEL", RuleKind.Condition, RuleMode.Write, "CEL", null, null, 
                        "message.name == '" + uuid + "'", null, null, false)
                }
            );
            store[schemaStr] = 1;
            subjectStore["topic-value"] = new List<RegisteredSchema> { schema }; 
            var config = new AvroSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = true
            };
            var serializer = new AvroSerializer<GenericRecord>(schemaRegistryClient, config);
            var deserializer = new AvroDeserializer<GenericRecord>(schemaRegistryClient, null);

            var user = new GenericRecord((RecordSchema) Avro.Schema.Parse(schemaStr));
            user.Add("name", uuid);
            user.Add("favorite_number", 100);
            user.Add("favorite_color", "blue");

            Headers headers = new Headers();
            var bytes = serializer.SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            var result = deserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;

            Assert.Equal(new Guid(uuid), result["name"]);
            Assert.Equal(user["favorite_color"], result["favorite_color"]);
            Assert.Equal(user["favorite_number"], result["favorite_number"]);
        }

        [Fact]
        public void GenericRecordCELConditionFail()
        {
            var schemaStr = User._SCHEMA.ToString();
            var schema = new RegisteredSchema("topic-value", 1, 1, schemaStr, SchemaType.Avro, null);
            schema.RuleSet = new RuleSet(new List<Rule>(),
                new List<Rule>
                {
                    new Rule("testCEL", RuleKind.Condition, RuleMode.Write, "CEL", null, null, 
                        "message.name != 'awesome'", null, null, false)
                }
            );
            store[schemaStr] = 1;
            subjectStore["topic-value"] = new List<RegisteredSchema> { schema }; 
            var config = new AvroSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = true
            };
            var serializer = new AvroSerializer<GenericRecord>(schemaRegistryClient, config);

            var user = new GenericRecord((RecordSchema) User._SCHEMA);
            user.Add("name", "awesome");
            user.Add("favorite_number", 100);
            user.Add("favorite_color", "blue");

            Headers headers = new Headers();
            Assert.Throws<AggregateException>(() => serializer.SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result);
        }

        [Fact]
        public void GenericRecordCELConditionEmail()
        {
            var schemaStr = User._SCHEMA.ToString();
            var schema = new RegisteredSchema("topic-value", 1, 1, schemaStr, SchemaType.Avro, null);
            schema.RuleSet = new RuleSet(new List<Rule>(),
                new List<Rule>
                {
                    new Rule("testCEL", RuleKind.Condition, RuleMode.Write, "CEL", null, null,
                        "message.name.isEmail()", null, null, false)
                }
            );
            store[schemaStr] = 1;
            subjectStore["topic-value"] = new List<RegisteredSchema> { schema };
            var config = new AvroSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = true
            };
            var serializer = new AvroSerializer<GenericRecord>(schemaRegistryClient, config);
            var deserializer = new AvroDeserializer<GenericRecord>(schemaRegistryClient, null);

            var user = new GenericRecord((RecordSchema) User._SCHEMA);
            user.Add("name", "bob@confluent.com");
            user.Add("favorite_number", 100);
            user.Add("favorite_color", "blue");

            Headers headers = new Headers();
            var bytes = serializer.SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            var result = deserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;

            Assert.Equal("bob@confluent.com", result["name"]);
            Assert.Equal(user["favorite_color"], result["favorite_color"]);
            Assert.Equal(user["favorite_number"], result["favorite_number"]);
        }

        [Fact]
        public void GenericRecordCELConditionEmailFail()
        {
            var schemaStr = User._SCHEMA.ToString();
            var schema = new RegisteredSchema("topic-value", 1, 1, schemaStr, SchemaType.Avro, null);
            schema.RuleSet = new RuleSet(new List<Rule>(),
                new List<Rule>
                {
                    new Rule("testCEL", RuleKind.Condition, RuleMode.Write, "CEL", null, null,
                        "message.name.isEmail()", null, null, false)
                }
            );
            store[schemaStr] = 1;
            subjectStore["topic-value"] = new List<RegisteredSchema> { schema };
            var config = new AvroSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = true
            };
            var serializer = new AvroSerializer<GenericRecord>(schemaRegistryClient, config);

            var user = new GenericRecord((RecordSchema) User._SCHEMA);
            user.Add("name", "awesome");
            user.Add("favorite_number", 100);
            user.Add("favorite_color", "blue");

            Headers headers = new Headers();
            Assert.Throws<AggregateException>(() => serializer.SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result);
        }

        [Fact]
        public void GenericRecordCELFieldTransform()
        {
            var schemaStr = User._SCHEMA.ToString();
            var schema = new RegisteredSchema("topic-value", 1, 1, schemaStr, SchemaType.Avro, null);
            schema.RuleSet = new RuleSet(new List<Rule>(),
                new List<Rule>
                {
                    new Rule("testCEL", RuleKind.Transform, RuleMode.Write, "CEL_FIELD", null, null, 
                        "typeName == 'STRING' ; value + '-suffix'", null, null, false)
                }
            );
            store[schemaStr] = 1;
            subjectStore["topic-value"] = new List<RegisteredSchema> { schema }; 
            var config = new AvroSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = true
            };
            var serializer = new AvroSerializer<GenericRecord>(schemaRegistryClient, config);
            var deserializer = new AvroDeserializer<GenericRecord>(schemaRegistryClient, null);

            var user = new GenericRecord((RecordSchema) User._SCHEMA);
            user.Add("name", "awesome");
            user.Add("favorite_number", 100);
            user.Add("favorite_color", "blue");

            Headers headers = new Headers();
            var bytes = serializer.SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            var result = deserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;

            Assert.Equal("awesome-suffix", result["name"]);
            Assert.Equal("blue-suffix", result["favorite_color"]);
            Assert.Equal(user["favorite_number"], result["favorite_number"]);
        }

        [Fact]
        public void GenericRecordCELFieldCondition()
        {
            var schemaStr = User._SCHEMA.ToString();
            var schema = new RegisteredSchema("topic-value", 1, 1, schemaStr, SchemaType.Avro, null);
            schema.RuleSet = new RuleSet(new List<Rule>(),
                new List<Rule>
                {
                    new Rule("testCEL", RuleKind.Condition, RuleMode.Write, "CEL_FIELD", null, null, 
                        "name == 'name' ; value == 'awesome'", null, null, false)
                }
            );
            store[schemaStr] = 1;
            subjectStore["topic-value"] = new List<RegisteredSchema> { schema }; 
            var config = new AvroSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = true
            };
            var serializer = new AvroSerializer<GenericRecord>(schemaRegistryClient, config);
            var deserializer = new AvroDeserializer<GenericRecord>(schemaRegistryClient, null);

            var user = new GenericRecord((RecordSchema) User._SCHEMA);
            user.Add("name", "awesome");
            user.Add("favorite_number", 100);
            user.Add("favorite_color", "blue");

            Headers headers = new Headers();
            var bytes = serializer.SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            var result = deserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;

            Assert.Equal("awesome", result["name"]);
            Assert.Equal(user["favorite_color"], result["favorite_color"]);
            Assert.Equal(user["favorite_number"], result["favorite_number"]);
        }

        [Fact]
        public void GenericRecordCELFieldConditionFail()
        {
            var schemaStr = User._SCHEMA.ToString();
            var schema = new RegisteredSchema("topic-value", 1, 1, schemaStr, SchemaType.Avro, null);
            schema.RuleSet = new RuleSet(new List<Rule>(),
                new List<Rule>
                {
                    new Rule("testCEL", RuleKind.Condition, RuleMode.Write, "CEL_FIELD", null, null, 
                        "name == 'name' ; value != 'awesome'", null, null, false)
                }
            );
            store[schemaStr] = 1;
            subjectStore["topic-value"] = new List<RegisteredSchema> { schema }; 
            var config = new AvroSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = true
            };
            var serializer = new AvroSerializer<GenericRecord>(schemaRegistryClient, config);

            var user = new GenericRecord((RecordSchema) User._SCHEMA);
            user.Add("name", "awesome");
            user.Add("favorite_number", 100);
            user.Add("favorite_color", "blue");

            Headers headers = new Headers();
            Assert.Throws<AggregateException>(() => serializer.SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result);
        }

        [Fact]
        public void GenericRecordFieldEncryption()
        {
            var schemaStr = "{\"type\":\"record\",\"name\":\"UserWithPic\",\"namespace\":\"Confluent.Kafka.Examples.AvroSpecific" +
                    "\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\"," +
                    "\"type\":[\"int\",\"null\"]},{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"]}," +
                    "{\"name\":\"picture\",\"type\":[\"null\",\"bytes\"],\"default\":null}]}";

            var schema = new RegisteredSchema("topic-value", 1, 1, schemaStr, SchemaType.Avro, null);
            schema.Metadata = new Metadata(new Dictionary<string, ISet<string>>
                {
                    ["Confluent.Kafka.Examples.AvroSpecific.UserWithPic.name"] = new HashSet<string> { "PII" },
                    ["Confluent.Kafka.Examples.AvroSpecific.UserWithPic.picture"] = new HashSet<string> { "PII" }

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
            var config = new AvroSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = true
            };
            config.Set("rules.secret", "mysecret");
            RuleRegistry ruleRegistry = new RuleRegistry();
            IRuleExecutor ruleExecutor = new FieldEncryptionExecutor(dekRegistryClient, clock);
            ruleRegistry.RegisterExecutor(ruleExecutor);
            var serializer = new AvroSerializer<GenericRecord>(schemaRegistryClient, config, ruleRegistry);
            var deserializer = new AvroDeserializer<GenericRecord>(schemaRegistryClient, null, ruleRegistry);

            var pic = new byte[] { 1, 2, 3 };
            var user = new GenericRecord((RecordSchema) UserWithPic._SCHEMA);
            user.Add("name", "awesome");
            user.Add("favorite_number", 100);
            user.Add("favorite_color", "blue");
            user.Add("picture", pic);

            Headers headers = new Headers();
            var bytes = serializer.SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            var result = deserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;

            Assert.Equal("awesome", result["name"]);
            Assert.Equal(user["favorite_color"], result["favorite_color"]);
            Assert.Equal(user["favorite_number"], result["favorite_number"]);
            Assert.True(pic.SequenceEqual((byte[])result["picture"]));
        }

        [Fact]
        public void GenericRecordFieldEncryptionF1Preserialized()
        {
            var schemaStr = "{\"type\":\"record\",\"name\":\"myrecord\"," +
                            "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}";

            var schema = new RegisteredSchema("topic-value", 1, 1, schemaStr, SchemaType.Avro, null);
            schema.Metadata = new Metadata(new Dictionary<string, ISet<string>>
                {
                    ["myrecord.f1"] = new HashSet<string> { "PII" },

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
            var config = new AvroDeserializerConfig
            {
            };
            config.Set("rules.secret", "mysecret");
            RuleRegistry ruleRegistry = new RuleRegistry();
            IRuleExecutor ruleExecutor = new FieldEncryptionExecutor(dekRegistryClient, clock);
            ruleRegistry.RegisterExecutor(ruleExecutor);
            var deserializer = new AvroDeserializer<GenericRecord>(schemaRegistryClient, config, ruleRegistry);

            kekStore[new KekId("kek1", false)] =
                new RegisteredKek()
                {
                    Name = "kek1",
                    KmsType = "local-kms",
                    KmsKeyId = "mykey",
                };
            dekStore[new DekId("kek1", "topic-value", 1, DekFormat.AES256_GCM, false)] =
                new RegisteredDek()
                {
                    Subject = "topic-value",
                    Version = 1,
                    Algorithm = DekFormat.AES256_GCM,
                    EncryptedKeyMaterial =
                        "07V2ndh02DA73p+dTybwZFm7DKQSZN1tEwQh+FoX1DZLk4Yj2LLu4omYjp/84tAg3BYlkfGSz+zZacJHIE4=",
                };
            Headers headers = new Headers();
            byte[] bytes = new byte[]{0, 0, 0, 0, 1, 104, 122, 103, 121, 47, 106, 70, 78, 77, 86, 47, 101, 70, 105, 108, 97, 72, 114, 77, 121, 101, 66, 103, 100, 97, 86, 122, 114, 82, 48, 117, 100, 71, 101, 111, 116, 87, 56, 99, 65, 47, 74, 97, 108, 55, 117, 107, 114, 43, 77, 47, 121, 122};
            var result = deserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;

            Assert.Equal("hello world", result["f1"]);
        }

        [Fact]
        public void GenericRecordFieldEncryptionDeterministicF1Preserialized()
        {
            var schemaStr = "{\"type\":\"record\",\"name\":\"myrecord\"," +
                            "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}";

            var schema = new RegisteredSchema("topic-value", 1, 1, schemaStr, SchemaType.Avro, null);
            schema.Metadata = new Metadata(new Dictionary<string, ISet<string>>
                {
                    ["myrecord.f1"] = new HashSet<string> { "PII" },

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
                        ["encrypt.kms.key.id"] = "mykey",
                        ["encrypt.dek.algorithm"] = "AES256_SIV",
                    })
                }
            );
            store[schemaStr] = 1;
            subjectStore["topic-value"] = new List<RegisteredSchema> { schema };
            var config = new AvroDeserializerConfig
            {
            };
            config.Set("rules.secret", "mysecret");
            RuleRegistry ruleRegistry = new RuleRegistry();
            IRuleExecutor ruleExecutor = new FieldEncryptionExecutor(dekRegistryClient, clock);
            ruleRegistry.RegisterExecutor(ruleExecutor);
            var deserializer = new AvroDeserializer<GenericRecord>(schemaRegistryClient, config, ruleRegistry);

            kekStore[new KekId("kek1", false)] =
                new RegisteredKek()
                {
                    Name = "kek1",
                    KmsType = "local-kms",
                    KmsKeyId = "mykey",
                };
            dekStore[new DekId("kek1", "topic-value", 1, DekFormat.AES256_SIV, false)] =
                new RegisteredDek()
                {
                    Subject = "topic-value",
                    Version = 1,
                    Algorithm = DekFormat.AES256_SIV,
                    EncryptedKeyMaterial =
                        "YSx3DTlAHrmpoDChquJMifmPntBzxgRVdMzgYL82rgWBKn7aUSnG+WIu9ozBNS3y2vXd++mBtK07w4/W/G6w0da39X9hfOVZsGnkSvry/QRht84V8yz3dqKxGMOK5A==",
                };
            Headers headers = new Headers();
            byte[] bytes = new byte[]{0, 0, 0, 0, 1, 72, 68, 54, 89, 116, 120, 114, 108, 66, 110, 107, 84, 87, 87, 57, 78, 54, 86, 98, 107, 51, 73, 73, 110, 106, 87, 72, 56, 49, 120, 109, 89, 104, 51, 107, 52, 100};
            var result = deserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;

            Assert.Equal("hello world", result["f1"]);
        }

        [Fact]
        public void GenericRecordFieldEncryptionDekRotationF1Preserialized()
        {
            var schemaStr = "{\"type\":\"record\",\"name\":\"myrecord\"," +
                            "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}";

            var schema = new RegisteredSchema("topic-value", 1, 1, schemaStr, SchemaType.Avro, null);
            schema.Metadata = new Metadata(new Dictionary<string, ISet<string>>
                {
                    ["myrecord.f1"] = new HashSet<string> { "PII" },

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
                        ["encrypt.kms.key.id"] = "mykey",
                        ["encrypt.dek.expiry.days"] = "1"
                    })
                }
            );
            store[schemaStr] = 1;
            subjectStore["topic-value"] = new List<RegisteredSchema> { schema };
            var config = new AvroDeserializerConfig
            {
            };
            config.Set("rules.secret", "mysecret");
            RuleRegistry ruleRegistry = new RuleRegistry();
            IRuleExecutor ruleExecutor = new FieldEncryptionExecutor(dekRegistryClient, clock);
            ruleRegistry.RegisterExecutor(ruleExecutor);
            var deserializer = new AvroDeserializer<GenericRecord>(schemaRegistryClient, config, ruleRegistry);

            kekStore[new KekId("kek1", false)] =
                new RegisteredKek()
                {
                    Name = "kek1",
                    KmsType = "local-kms",
                    KmsKeyId = "mykey",
                };
            dekStore[new DekId("kek1", "topic-value", 1, DekFormat.AES256_GCM, false)] =
                new RegisteredDek()
                {
                    Subject = "topic-value",
                    Version = 1,
                    Algorithm = DekFormat.AES256_GCM,
                    EncryptedKeyMaterial =
                        "W/v6hOQYq1idVAcs1pPWz9UUONMVZW4IrglTnG88TsWjeCjxmtRQ4VaNe/I5dCfm2zyY9Cu0nqdvqImtUk4=",
                };
            Headers headers = new Headers();
            byte[] bytes = new byte[]{0, 0, 0, 0, 1, 120, 65, 65, 65, 65, 65, 65, 71, 52, 72, 73, 54, 98, 49, 110, 88, 80, 88, 113, 76, 121, 71, 56, 99, 73, 73, 51, 53, 78, 72, 81, 115, 101, 113, 113, 85, 67, 100, 43, 73, 101, 76, 101, 70, 86, 65, 101, 78, 112, 83, 83, 51, 102, 120, 80, 110, 74, 51, 50, 65, 61};
            var result = deserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;

            Assert.Equal("hello world", result["f1"]);
        }

        [Fact]
        public void GenericRecordJSONataWithCEL()
        {
            var rule1To2 = "$merge([$sift($, function($v, $k) {$k != 'name'}), {'full_name': $.'name'}])";

            var schemaStr = User._SCHEMA.ToString();
            var schema = new RegisteredSchema("topic-value", 1, 1, schemaStr, SchemaType.Avro, null);
            schema.Metadata = new Metadata(null, new Dictionary<string, string>
                {
                    { "application.version", "1"}

                }, new HashSet<string>()
            );
            store[schemaStr] = 1;
            var config1 = new AvroSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = false,
                UseLatestWithMetadata = new Dictionary<string, string>{ { "application.version", "1"} }
            };
            var serializer1 = new AvroSerializer<GenericRecord>(schemaRegistryClient, config1);

            var user = new GenericRecord((RecordSchema) User._SCHEMA);
            user.Add("name", "awesome");
            user.Add("favorite_number", 100);
            user.Add("favorite_color", "blue");

            var newSchemaStr = NewUser._SCHEMA.ToString();
            var newSchema = new RegisteredSchema("topic-value", 2, 2, newSchemaStr, SchemaType.Avro, null);
            newSchema.Metadata = new Metadata(null, new Dictionary<string, string>
                {
                    { "application.version", "2"}

                }, new HashSet<string>()
            );
            newSchema.RuleSet = new RuleSet(
                new List<Rule>
                {
                    new Rule("myRule1", RuleKind.Transform, RuleMode.Upgrade, "JSONATA", null,
                        null, rule1To2, null, null, false)
                }, new List<Rule>
                {
                    new Rule("myRule2", RuleKind.Transform, RuleMode.Read, "CEL_FIELD", null,
                        null, "name == 'full_name' ; value + '-suffix'", null, null, false)
                }
            );
            var deserConfig2 = new AvroDeserializerConfig
            {
                UseLatestVersion = false,
                UseLatestWithMetadata = new Dictionary<string, string>{ { "application.version", "2"} }
            };
            var deserializer2 = new AvroDeserializer<GenericRecord>(schemaRegistryClient, deserConfig2);

            store[schemaStr] = 1;
            store[newSchemaStr] = 2;
            subjectStore["topic-value"] = new List<RegisteredSchema> { schema, newSchema };

            Headers headers = new Headers();
            var bytes = serializer1.SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;

            var result2 = deserializer2.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;

            Assert.Equal("awesome-suffix", result2["full_name"]);
            Assert.Equal(user["favorite_color"], result2["favorite_color"]);
            Assert.Equal(user["favorite_number"], result2["favorite_number"]);
        }

        [Fact]
        public void GenericRecordJSONataFullyCompatible()
        {
            var rule1To2 = "$merge([$sift($, function($v, $k) {$k != 'name'}), {'full_name': $.'name'}])";
            var rule2To1 = "$merge([$sift($, function($v, $k) {$k != 'full_name'}), {'name': $.'full_name'}])";
            var rule2To3 = "$merge([$sift($, function($v, $k) {$k != 'full_name'}), {'title': $.'full_name'}])";
            var rule3To2 = "$merge([$sift($, function($v, $k) {$k != 'title'}), {'full_name': $.'title'}])";

            var schemaStr = User._SCHEMA.ToString();
            var schema = new RegisteredSchema("topic-value", 1, 1, schemaStr, SchemaType.Avro, null);
            schema.Metadata = new Metadata(null, new Dictionary<string, string>
                {
                    { "application.version", "1"}
                    
                }, new HashSet<string>()
            );
            store[schemaStr] = 1;
            var config1 = new AvroSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = false,
                UseLatestWithMetadata = new Dictionary<string, string>{ { "application.version", "1"} }
            };
            var deserConfig1 = new AvroDeserializerConfig
            {
                UseLatestVersion = false,
                UseLatestWithMetadata = new Dictionary<string, string>{ { "application.version", "1"} }
            };
            var serializer1 = new AvroSerializer<GenericRecord>(schemaRegistryClient, config1);
            var deserializer1 = new AvroDeserializer<GenericRecord>(schemaRegistryClient, deserConfig1);

            var user = new GenericRecord((RecordSchema) User._SCHEMA);
            user.Add("name", "awesome");
            user.Add("favorite_number", 100);
            user.Add("favorite_color", "blue");

            var newSchemaStr = NewUser._SCHEMA.ToString();
            var newSchema = new RegisteredSchema("topic-value", 2, 2, newSchemaStr, SchemaType.Avro, null);
            newSchema.Metadata = new Metadata(null, new Dictionary<string, string>
                {
                    { "application.version", "2"}
                    
                }, new HashSet<string>()
            );
            newSchema.RuleSet = new RuleSet(
                new List<Rule>
                {
                    new Rule("myRule1", RuleKind.Transform, RuleMode.Upgrade, "JSONATA", null, 
                        null, rule1To2, null, null, false),
                    new Rule("myRule2", RuleKind.Transform, RuleMode.Downgrade, "JSONATA", null, 
                        null, rule2To1, null, null, false)
                }, new List<Rule>()
            );
            var config2 = new AvroSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = false,
                UseLatestWithMetadata = new Dictionary<string, string>{ { "application.version", "2"} }
            };
            var deserConfig2 = new AvroDeserializerConfig
            {
                UseLatestVersion = false,
                UseLatestWithMetadata = new Dictionary<string, string>{ { "application.version", "2"} }
            };
            var serializer2 = new AvroSerializer<GenericRecord>(schemaRegistryClient, config2);
            var deserializer2 = new AvroDeserializer<GenericRecord>(schemaRegistryClient, deserConfig2);

            var newUser = new GenericRecord((RecordSchema) NewUser._SCHEMA);
            newUser.Add("full_name", "awesome");
            newUser.Add("favorite_number", 100);
            newUser.Add("favorite_color", "blue");

            var newerSchemaStr = NewerUser._SCHEMA.ToString();
            var newerSchema = new RegisteredSchema("topic-value", 3, 3, newerSchemaStr, SchemaType.Avro, null);
            newerSchema.Metadata = new Metadata(null, new Dictionary<string, string>
                {
                    { "application.version", "3"}
                    
                }, new HashSet<string>()
            );
            newerSchema.RuleSet = new RuleSet(
                new List<Rule>
                {
                    new Rule("myRule1", RuleKind.Transform, RuleMode.Upgrade, "JSONATA", null, 
                        null, rule2To3, null, null, false),
                    new Rule("myRule2", RuleKind.Transform, RuleMode.Downgrade, "JSONATA", null, 
                        null, rule3To2, null, null, false)
                }, new List<Rule>()
            );
            var config3 = new AvroSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = false,
                UseLatestWithMetadata = new Dictionary<string, string>{ { "application.version", "3"} }
            };
            var deserConfig3 = new AvroDeserializerConfig
            {
                UseLatestVersion = false,
                UseLatestWithMetadata = new Dictionary<string, string>{ { "application.version", "3"} }
            };
            var serializer3 = new AvroSerializer<GenericRecord>(schemaRegistryClient, config3);
            var deserializer3 = new AvroDeserializer<GenericRecord>(schemaRegistryClient, deserConfig3);

            var newerUser = new GenericRecord((RecordSchema) NewerUser._SCHEMA);
            newerUser.Add("title", "awesome");
            newerUser.Add("favorite_number", 100);
            newerUser.Add("favorite_color", "blue");

            store[schemaStr] = 1;
            store[newSchemaStr] = 2;
            store[newerSchemaStr] = 3;
            subjectStore["topic-value"] = new List<RegisteredSchema> { schema, newSchema, newerSchema }; 

            Headers headers = new Headers();
            var bytes = serializer1.SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            GenericRecordDeserializeAllVersions(deserializer1, deserializer2, deserializer3, bytes, headers, user);
            
            bytes = serializer2.SerializeAsync(newUser, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            GenericRecordDeserializeAllVersions(deserializer1, deserializer2, deserializer3, bytes, headers, user);
            
            bytes = serializer3.SerializeAsync(newerUser, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            GenericRecordDeserializeAllVersions(deserializer1, deserializer2, deserializer3, bytes, headers, user);
        }

        private void GenericRecordDeserializeAllVersions(AvroDeserializer<GenericRecord> deserializer1, 
            AvroDeserializer<GenericRecord> deserializer2, AvroDeserializer<GenericRecord> deserializer3, 
            byte[] bytes, Headers headers, GenericRecord user)
        {
            var result1 = deserializer1.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            var result2 = deserializer2.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            var result3 = deserializer3.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;

            Assert.Equal("awesome", result1["name"]);
            Assert.Equal(user["favorite_color"], result1["favorite_color"]);
            Assert.Equal(user["favorite_number"], result1["favorite_number"]);

            Assert.Equal("awesome", result2["full_name"]);
            Assert.Equal(user["favorite_color"], result2["favorite_color"]);
            Assert.Equal(user["favorite_number"], result2["favorite_number"]);

            Assert.Equal("awesome", result3["title"]);
            Assert.Equal(user["favorite_color"], result3["favorite_color"]);
            Assert.Equal(user["favorite_number"], result3["favorite_number"]);
        }

        [Fact]
        public void NullISpecificRecord()
        {
            var serializer = new AvroSerializer<User>(schemaRegistryClient);
            var deserializer = new AvroDeserializer<User>(schemaRegistryClient);

            var bytes = serializer.SerializeAsync(null, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            var result = deserializer.DeserializeAsync(bytes, isNull: true, new SerializationContext(MessageComponentType.Value, testTopic)).Result;

            Assert.Null(bytes);
            Assert.Null(result);
        }

        [Fact]
        public void NullGenericRecord()
        {
            var serializer = new AvroSerializer<GenericRecord>(schemaRegistryClient);
            var deserializer = new AvroDeserializer<GenericRecord>(schemaRegistryClient);

            var bytes = serializer.SerializeAsync(null, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            var result = deserializer.DeserializeAsync(bytes, isNull: true, new SerializationContext(MessageComponentType.Value, testTopic)).Result;

            Assert.Null(bytes);
            Assert.Null(result);
        }

        [Fact]
        public void NullString()
        {
            var serializer = new AvroSerializer<string>(schemaRegistryClient);
            var deserializer = new AvroDeserializer<string>(schemaRegistryClient);

            var bytes = serializer.SerializeAsync(null, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            var result = deserializer.DeserializeAsync(bytes, isNull: true, new SerializationContext(MessageComponentType.Value, testTopic)).Result;

            Assert.Null(bytes);
            Assert.Null(result);
        }

        [Fact]
        public void NullInt()
        {
            var deserializer = new AvroDeserializer<int>(schemaRegistryClient);

            var exception = Assert.Throws<AggregateException>(() => deserializer.DeserializeAsync(ReadOnlyMemory<byte>.Empty, isNull: true, new SerializationContext(MessageComponentType.Value, testTopic)).Result);

            Assert.Equal("Cannot deserialize null to a Value Type", exception.InnerException.Message);
        }

        [Fact]
        public void Multiple_ISpecificRecords()
        {
            var serializer = new AvroSerializer<ISpecificRecord>(schemaRegistryClient);
            var deserializerUser = new AvroDeserializer<User>(schemaRegistryClient);
            var deserializerCar = new AvroDeserializer<Car>(schemaRegistryClient);

            var user = new User
            {
                favorite_color = "blue",
                favorite_number = 100,
                name = "awesome"
            };

            var car = new Car
            {
                color = "blue",
                name = "great_brand"
            };

            var bytesUser = serializer.SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            var resultUser = deserializerUser.DeserializeAsync(bytesUser, false, new SerializationContext(MessageComponentType.Value, testTopic)).Result as User;

            Assert.NotNull(resultUser);
            Assert.Equal(user.name, resultUser.name);
            Assert.Equal(user.favorite_color, resultUser.favorite_color);
            Assert.Equal(user.favorite_number, resultUser.favorite_number);

            var bytesCar = serializer.SerializeAsync(car, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            var resultCar = deserializerCar.DeserializeAsync(bytesCar, false, new SerializationContext(MessageComponentType.Value, testTopic)).Result as Car;

            Assert.NotNull(resultCar);
            Assert.Equal(car.name, resultCar.name);
            Assert.Equal(car.color, resultCar.color);
        }

        [Fact]
        public void Poco_Serialize()
        {
            var serializer = new AvroSerializer<Dictionary<string, string>>(schemaRegistryClient);
            Assert.Throws<System.InvalidOperationException>(() => serializer.SerializeAsync(new Dictionary<string, string> { { "cat", "dog" } }, new SerializationContext(MessageComponentType.Key, testTopic)).GetAwaiter().GetResult());
        }

        [Fact]
        public void Poco_Deserialize()
        {
            var deserializer = new AvroDeserializer<Dictionary<string, string>>(schemaRegistryClient);
            Assert.Throws<System.InvalidOperationException>(() => deserializer.DeserializeAsync(new System.ReadOnlyMemory<byte>(new byte[] { 1, 2, 3 }), false, new SerializationContext(MessageComponentType.Key, testTopic)).GetAwaiter().GetResult());
        }

        [Fact]
        public void Incompatible()
        {
            var avroSerializer = new AvroSerializer<string>(schemaRegistryClient);
            var avroDeserializer = new AvroDeserializer<int>(schemaRegistryClient);
            var bytes = avroSerializer.SerializeAsync("hello world", new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            Assert.Throws<System.AggregateException>(() => avroDeserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic)).Result);
        }

        /// <summary>
        /// Test a case when .NET data class name and / or namespace do not match the schema name and / or namespace.
        /// </summary>
        [Fact]
        public void ISpecificRecord_SchemaTypeMismatch()
        {
            var serializer = new AvroSerializer<User2>(schemaRegistryClient);
            var deserializer = new AvroDeserializer<User2>(schemaRegistryClient);

            var user = new User2
            {
                favorite_color = "blue",
                favorite_number = 100,
                name = "awesome"
            };

            var bytes = serializer.SerializeAsync(user, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            var result = deserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic)).Result;

            Assert.Equal(user.name, result.name);
            Assert.Equal(user.favorite_color, result.favorite_color);
            Assert.Equal(user.favorite_number, result.favorite_number);
        }
    }
}
