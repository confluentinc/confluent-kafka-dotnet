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

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry.Rules;
using Xunit;

namespace Confluent.SchemaRegistry.Serdes.UnitTests
{
    /// <summary>
    ///     Serializer-level tests for inline validation rules — these exercise the
    ///     validation.rules.execution wiring. Per-rule CEL semantics are covered in
    ///     CelValidatorTests.
    /// </summary>
    /// <summary>
    ///     Public POCO: Cel.NET builds its type descriptor by reflection and cannot see
    ///     non-public types.
    /// </summary>
    public class ValidationCustomer
    {
        [Newtonsoft.Json.JsonProperty("favorite_color")]
        public string FavoriteColor { get; set; }

        [Newtonsoft.Json.JsonProperty("favorite_number")]
        public int FavoriteNumber { get; set; }

        [Newtonsoft.Json.JsonProperty("name")]
        public string Name { get; set; }
    }

    public class NestedRuleChild
    {
        [Newtonsoft.Json.JsonProperty("code")]
        public string Code { get; set; }
    }

    public class NestedRuleParent
    {
        [Newtonsoft.Json.JsonProperty("child")]
        public NestedRuleChild Child { get; set; }
    }

    public class ValidationRuleSerdeTests : BaseSerializeDeserializeTests
    {
        public ValidationRuleSerdeTests() : base()
        {
            CelValidator.Register();
        }

        /// <summary>
        ///     A registry carrying the CEL validator. Rule registries are isolated, so a
        ///     serializer given its own registry needs the validator registered on it.
        /// </summary>
        private static RuleRegistry ValidatingRegistry()
        {
            var registry = new RuleRegistry();
            registry.RegisterValidationExecutor(new CelValidator());
            return registry;
        }

        // Record-level rule plus two field-level rules, matching the JVM client's layout.
        private const string AvroValidationSchema = @"{
            ""type"": ""record"",
            ""name"": ""User"",
            ""namespace"": ""Confluent.Kafka.Examples.AvroSpecific"",
            ""confluent:rules"": [
                { ""name"": ""nameNotForbidden"", ""expr"": ""this.name != 'forbidden'"" }
            ],
            ""fields"": [
                {
                    ""name"": ""name"",
                    ""type"": ""string"",
                    ""confluent:rules"": [
                        { ""name"": ""nameNotEmpty"", ""doc"": ""name must not be empty"", ""expr"": ""size(this) > 0"" }
                    ]
                },
                {
                    ""name"": ""favorite_number"",
                    ""type"": [""int"", ""null""],
                    ""confluent:rules"": [
                        { ""name"": ""numberPositive"", ""expr"": ""this >= 0"" }
                    ]
                },
                { ""name"": ""favorite_color"", ""type"": [""string"", ""null""] }
            ]
        }";

        private RegisteredSchema RegisterAvro()
        {
            var schema = new RegisteredSchema("topic-value", 1, 1, AvroValidationSchema, SchemaType.Avro, null);
            store[AvroValidationSchema] = 1;
            subjectStore["topic-value"] = new List<RegisteredSchema> { schema };
            return schema;
        }

        private AvroSerializer<T> AvroSerializerFor<T>(ValidationRulesExecution execution, bool failFast = false)
        {
            RegisterAvro();
            var config = new AvroSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = true,
                ValidationRulesExecution = execution,
                ValidationRulesFailFast = failFast
            };
            return new AvroSerializer<T>(schemaRegistryClient, config);
        }

        private static Confluent.Kafka.Examples.AvroSpecific.User User(string name, int? number) =>
            new Confluent.Kafka.Examples.AvroSpecific.User
            {
                name = name,
                favorite_number = number,
                favorite_color = "blue"
            };

        [Fact]
        public void AvroPassesWhenAllRulesPass()
        {
            var serializer = AvroSerializerFor<Confluent.Kafka.Examples.AvroSpecific.User>(
                ValidationRulesExecution.AfterDomainRules);
            var headers = new Headers();
            var bytes = serializer.SerializeAsync(User("Alice", 30),
                new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            Assert.True(bytes.Length > 0);
        }

        [Fact]
        public void AvroPassesWhenValidationDisabled()
        {
            // favorite_number -5 would fail numberPositive, but validation defaults to off.
            var serializer = AvroSerializerFor<Confluent.Kafka.Examples.AvroSpecific.User>(
                ValidationRulesExecution.Disabled);
            var headers = new Headers();
            var bytes = serializer.SerializeAsync(User("Alice", -5),
                new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            Assert.True(bytes.Length > 0);
        }

        [Fact]
        public void AvroFailsOnFieldRule()
        {
            var serializer = AvroSerializerFor<Confluent.Kafka.Examples.AvroSpecific.User>(
                ValidationRulesExecution.AfterDomainRules);
            var headers = new Headers();
            var ex = Assert.Throws<AggregateException>(() => serializer.SerializeAsync(User("Alice", -5),
                new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result);
            var failure = Assert.IsType<ValidationRulesFailedException>(ex.InnerException);
            Assert.Contains("favorite_number: numberPositive", failure.Message);
        }

        [Fact]
        public void AvroFailsOnRecordRule()
        {
            var serializer = AvroSerializerFor<Confluent.Kafka.Examples.AvroSpecific.User>(
                ValidationRulesExecution.AfterDomainRules);
            var headers = new Headers();
            var ex = Assert.Throws<AggregateException>(() => serializer.SerializeAsync(User("forbidden", 30),
                new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result);
            var failure = Assert.IsType<ValidationRulesFailedException>(ex.InnerException);
            Assert.Contains("<root>: nameNotForbidden", failure.Message);
        }

        [Fact]
        public void AvroReportsEveryViolation()
        {
            var serializer = AvroSerializerFor<Confluent.Kafka.Examples.AvroSpecific.User>(
                ValidationRulesExecution.AfterDomainRules);
            var headers = new Headers();
            var ex = Assert.Throws<AggregateException>(() => serializer.SerializeAsync(User("", -5),
                new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result);
            var failure = Assert.IsType<ValidationRulesFailedException>(ex.InnerException);
            Assert.Equal(2, failure.Violations.Count);
            Assert.Contains("2 violations", failure.Message);
            Assert.Contains("name must not be empty", failure.Message);
            Assert.Contains("numberPositive", failure.Message);
        }

        [Fact]
        public void AvroFailFastReportsOneViolation()
        {
            var serializer = AvroSerializerFor<Confluent.Kafka.Examples.AvroSpecific.User>(
                ValidationRulesExecution.AfterDomainRules, failFast: true);
            var headers = new Headers();
            var ex = Assert.Throws<AggregateException>(() => serializer.SerializeAsync(User("", -5),
                new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result);
            var failure = Assert.IsType<ValidationRulesFailedException>(ex.InnerException);
            Assert.Single(failure.Violations);
            Assert.Contains("1 violation)", failure.Message);
        }

        [Fact]
        public void AvroBeforeDomainRulesAlsoValidates()
        {
            // No domain rules exist, so before and after collapse to the same single point.
            var serializer = AvroSerializerFor<Confluent.Kafka.Examples.AvroSpecific.User>(
                ValidationRulesExecution.BeforeDomainRules);
            var headers = new Headers();
            var ex = Assert.Throws<AggregateException>(() => serializer.SerializeAsync(User("Alice", -5),
                new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result);
            Assert.IsType<ValidationRulesFailedException>(ex.InnerException);
        }

        [Fact]
        public void NullableFieldRuleIsSkippedWhenNull()
        {
            // favorite_number is a nullable union; a null value must not invoke its rule.
            var serializer = AvroSerializerFor<Confluent.Kafka.Examples.AvroSpecific.User>(
                ValidationRulesExecution.AfterDomainRules);
            var headers = new Headers();
            var bytes = serializer.SerializeAsync(User("Alice", null),
                new SerializationContext(MessageComponentType.Value, testTopic, headers)).Result;
            Assert.True(bytes.Length > 0);
        }

        [Fact]
        public void ValidationRulesExecutionRoundTripsThroughConfig()
        {
            var config = new AvroSerializerConfig();
            Assert.Equal(ValidationRulesExecution.Disabled, config.ValidationRulesExecution);
            Assert.False(config.ValidationRulesFailFast);

            config.ValidationRulesExecution = ValidationRulesExecution.AfterDomainRules;
            config.ValidationRulesFailFast = true;
            Assert.Equal("AFTER_DOMAIN_RULES",
                config.Get(SerdeConfig.SharedPropertyNames.ValidationRulesExecution));
            Assert.Equal(ValidationRulesExecution.AfterDomainRules, config.ValidationRulesExecution);
            Assert.True(config.ValidationRulesFailFast);
        }

        [Fact]
        public void UnknownValidationRulesExecutionValueIsRejected()
        {
            var config = new AvroSerializerConfig();
            config.Set(SerdeConfig.SharedPropertyNames.ValidationRulesExecution, "NOPE");
            Assert.Throws<ArgumentException>(() => config.ValidationRulesExecution);
        }

        // ------------------------------------------------------------------------------
        // JSON Schema
        // ------------------------------------------------------------------------------

        // Object-level rule plus two property-level rules.
        private const string JsonValidationSchema = @"{
            ""type"": ""object"",
            ""title"": ""Customer"",
            ""confluent:rules"": [
                { ""name"": ""nameNotForbidden"", ""expr"": ""this.name != 'forbidden'"" }
            ],
            ""properties"": {
                ""favorite_color"": { ""type"": ""string"" },
                ""favorite_number"": {
                    ""type"": ""integer"",
                    ""confluent:rules"": [
                        { ""name"": ""numberPositive"", ""expr"": ""this >= 0"" }
                    ]
                },
                ""name"": {
                    ""type"": ""string"",
                    ""confluent:rules"": [
                        { ""name"": ""nameNotEmpty"", ""doc"": ""name must not be empty"", ""expr"": ""size(this) > 0"" }
                    ]
                }
            }
        }";

        private JsonSerializer<ValidationCustomer> JsonSerializerFor(ValidationRulesExecution execution,
            bool failFast = false)
        {
            var schema = new RegisteredSchema("topic-value", 1, 1, JsonValidationSchema, SchemaType.Json, null);
            store[JsonValidationSchema] = 1;
            subjectStore["topic-value"] = new List<RegisteredSchema> { schema };
            var config = new JsonSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = true,
                ValidationRulesExecution = execution,
                ValidationRulesFailFast = failFast
            };
            return new JsonSerializer<ValidationCustomer>(schemaRegistryClient, config, null, ValidatingRegistry());
        }

        private static ValidationCustomer JsonCustomer(string name, int number) =>
            new ValidationCustomer { Name = name, FavoriteNumber = number, FavoriteColor = "blue" };

        [Fact]
        public async Task JsonPassesWhenAllRulesPass()
        {
            var serializer = JsonSerializerFor(ValidationRulesExecution.AfterDomainRules);
            var bytes = await serializer.SerializeAsync(JsonCustomer("Alice", 30),
                new SerializationContext(MessageComponentType.Value, testTopic));
            Assert.True(bytes.Length > 0);
        }

        [Fact]
        public async Task JsonPassesWhenValidationDisabled()
        {
            var serializer = JsonSerializerFor(ValidationRulesExecution.Disabled);
            var bytes = await serializer.SerializeAsync(JsonCustomer("Alice", -5),
                new SerializationContext(MessageComponentType.Value, testTopic));
            Assert.True(bytes.Length > 0);
        }

        [Fact]
        public async Task JsonFailsOnPropertyRule()
        {
            var serializer = JsonSerializerFor(ValidationRulesExecution.AfterDomainRules);
            var ex = await Assert.ThrowsAsync<ValidationRulesFailedException>(() =>
                serializer.SerializeAsync(JsonCustomer("Alice", -5),
                    new SerializationContext(MessageComponentType.Value, testTopic)));
            // JSON paths are rooted at $, matching the JVM client.
            Assert.Contains("$.favorite_number: numberPositive", ex.Message);
        }

        [Fact]
        public async Task JsonFailsOnObjectRule()
        {
            var serializer = JsonSerializerFor(ValidationRulesExecution.AfterDomainRules);
            var ex = await Assert.ThrowsAsync<ValidationRulesFailedException>(() =>
                serializer.SerializeAsync(JsonCustomer("forbidden", 30),
                    new SerializationContext(MessageComponentType.Value, testTopic)));
            Assert.Contains("$: nameNotForbidden", ex.Message);
        }

        [Fact]
        public async Task JsonReportsEveryViolation()
        {
            var serializer = JsonSerializerFor(ValidationRulesExecution.AfterDomainRules);
            var ex = await Assert.ThrowsAsync<ValidationRulesFailedException>(() =>
                serializer.SerializeAsync(JsonCustomer("", -5),
                    new SerializationContext(MessageComponentType.Value, testTopic)));
            Assert.Equal(2, ex.Violations.Count);
            Assert.Contains("2 violations", ex.Message);
            Assert.Contains("name must not be empty", ex.Message);
            Assert.Contains("numberPositive", ex.Message);
        }

        [Fact]
        public async Task JsonFailFastReportsOneViolation()
        {
            var serializer = JsonSerializerFor(ValidationRulesExecution.AfterDomainRules, failFast: true);
            var ex = await Assert.ThrowsAsync<ValidationRulesFailedException>(() =>
                serializer.SerializeAsync(JsonCustomer("", -5),
                    new SerializationContext(MessageComponentType.Value, testTopic)));
            Assert.Single(ex.Violations);
            Assert.Contains("1 violation)", ex.Message);
        }

        // ------------------------------------------------------------------------------
        // Protobuf
        // ------------------------------------------------------------------------------

        // The rules live in the registered schema text; the generated C# class needs no
        // knowledge of them. Rules use the bracketed list form (`rules: [{...}]`) shared with
        // the other clients (JVM, Go, Python, JavaScript).
        private const string ProtobufValidationSchema = @"syntax = ""proto3"";
        import ""confluent/meta.proto"";

        package example;

        message PersonWithPic {
            option (.confluent.message_meta) = {
                rules: [{name: ""nameNotForbidden"", expr: ""this.name != 'forbidden'""}]
            };

            string favorite_color = 1;
            int32 favorite_number = 2 [(.confluent.field_meta) = {
                rules: [{name: ""numberPositive"", doc: ""number must not be negative"", expr: ""this >= 0""}]
            }];
            string name = 3 [(.confluent.field_meta) = {
                rules: [{name: ""nameNotEmpty"", expr: ""size(this) > 0""}]
            }];
            bytes picture = 4;
        }";

        private ProtobufSerializer<Example.PersonWithPic> ProtobufSerializerFor(
            ValidationRulesExecution execution, bool failFast = false)
        {
            var schema = new RegisteredSchema("topic-value", 1, 1, ProtobufValidationSchema,
                SchemaType.Protobuf, null);
            store[ProtobufValidationSchema] = 1;
            subjectStore["topic-value"] = new List<RegisteredSchema> { schema };
            var config = new ProtobufSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = true,
                ValidationRulesExecution = execution,
                ValidationRulesFailFast = failFast
            };
            return new ProtobufSerializer<Example.PersonWithPic>(schemaRegistryClient, config,
                ValidatingRegistry());
        }

        private static Example.PersonWithPic ProtoPerson(string name, int number) =>
            new Example.PersonWithPic
            {
                Name = name,
                FavoriteNumber = number,
                FavoriteColor = "blue"
            };

        [Fact]
        public async Task ProtobufPassesWhenAllRulesPass()
        {
            var serializer = ProtobufSerializerFor(ValidationRulesExecution.AfterDomainRules);
            var bytes = await serializer.SerializeAsync(ProtoPerson("Alice", 30),
                new SerializationContext(MessageComponentType.Value, testTopic, new Headers()));
            Assert.True(bytes.Length > 0);
        }

        [Fact]
        public async Task ProtobufPassesWhenValidationDisabled()
        {
            var serializer = ProtobufSerializerFor(ValidationRulesExecution.Disabled);
            var bytes = await serializer.SerializeAsync(ProtoPerson("Alice", -5),
                new SerializationContext(MessageComponentType.Value, testTopic, new Headers()));
            Assert.True(bytes.Length > 0);
        }

        [Fact]
        public async Task ProtobufFailsOnFieldRule()
        {
            var serializer = ProtobufSerializerFor(ValidationRulesExecution.AfterDomainRules);
            var ex = await Assert.ThrowsAsync<ValidationRulesFailedException>(() =>
                serializer.SerializeAsync(ProtoPerson("Alice", -5),
                    new SerializationContext(MessageComponentType.Value, testTopic, new Headers())));
            // The rule's doc is preferred over its expression in the failure text.
            Assert.Contains("number must not be negative", ex.Message);
        }

        [Fact]
        public async Task ProtobufFailsOnMessageRule()
        {
            var serializer = ProtobufSerializerFor(ValidationRulesExecution.AfterDomainRules);
            var ex = await Assert.ThrowsAsync<ValidationRulesFailedException>(() =>
                serializer.SerializeAsync(ProtoPerson("forbidden", 30),
                    new SerializationContext(MessageComponentType.Value, testTopic, new Headers())));
            Assert.Contains("<root>: nameNotForbidden", ex.Message);
        }

        [Fact]
        public async Task ProtobufReportsEveryViolation()
        {
            var serializer = ProtobufSerializerFor(ValidationRulesExecution.AfterDomainRules);
            var ex = await Assert.ThrowsAsync<ValidationRulesFailedException>(() =>
                serializer.SerializeAsync(ProtoPerson("", -5),
                    new SerializationContext(MessageComponentType.Value, testTopic, new Headers())));
            Assert.Equal(2, ex.Violations.Count);
            Assert.Contains("2 violations", ex.Message);
            Assert.Contains("numberPositive", ex.Message);
            Assert.Contains("nameNotEmpty", ex.Message);
        }

        [Fact]
        public async Task ProtobufFailFastReportsOneViolation()
        {
            var serializer = ProtobufSerializerFor(ValidationRulesExecution.AfterDomainRules,
                failFast: true);
            var ex = await Assert.ThrowsAsync<ValidationRulesFailedException>(() =>
                serializer.SerializeAsync(ProtoPerson("", -5),
                    new SerializationContext(MessageComponentType.Value, testTopic, new Headers())));
            Assert.Single(ex.Violations);
            Assert.Contains("1 violation)", ex.Message);
        }

        // ------------------------------------------------------------------------------
        // Auto-register path: no reader schema is selected, so before/after collapse to a
        // single validation point. Regression tests for validation being skipped entirely
        // when UseLatestVersion was not set.
        // ------------------------------------------------------------------------------

        [Fact]
        public void AvroValidatesWhenNoReaderSchemaIsSelected()
        {
            var writerSchema = global::Avro.Schema.Parse(AvroValidationSchema);
            var config = new AvroSerializerConfig
            {
                AutoRegisterSchemas = true,
                ValidationRulesExecution = ValidationRulesExecution.AfterDomainRules
            };
            var serializer = new AvroSerializer<GenericRecord>(schemaRegistryClient, config,
                ValidatingRegistry());
            var headers = new Headers();
            var context = new SerializationContext(MessageComponentType.Value, testTopic, headers);

            var valid = new GenericRecord((RecordSchema)writerSchema);
            valid.Add("name", "Alice");
            valid.Add("favorite_number", 30);
            valid.Add("favorite_color", "blue");
            Assert.True(serializer.SerializeAsync(valid, context).Result.Length > 0);

            var invalid = new GenericRecord((RecordSchema)writerSchema);
            invalid.Add("name", "");
            invalid.Add("favorite_number", 30);
            invalid.Add("favorite_color", "blue");
            var ex = Assert.Throws<AggregateException>(() =>
                serializer.SerializeAsync(invalid, context).Result);
            Assert.Contains("name must not be empty", ex.InnerException.Message);
        }

        [Fact]
        public void JsonValidatesWhenNoReaderSchemaIsSelected()
        {
            var config = new JsonSerializerConfig
            {
                AutoRegisterSchemas = true,
                ValidationRulesExecution = ValidationRulesExecution.AfterDomainRules
            };
            var serializer = new JsonSerializer<ValidationCustomer>(schemaRegistryClient,
                new Schema(JsonValidationSchema, SchemaType.Json), config, null,
                ValidatingRegistry());
            var headers = new Headers();
            var context = new SerializationContext(MessageComponentType.Value, testTopic, headers);

            Assert.True(serializer.SerializeAsync(JsonCustomer("Alice", 30), context)
                .Result.Length > 0);

            var ex = Assert.Throws<AggregateException>(() =>
                serializer.SerializeAsync(JsonCustomer("", 30), context).Result);
            Assert.Contains("name must not be empty", ex.InnerException.Message);
        }
        [Fact]
        public void ProtobufValidatesWhenNoReaderSchemaIsSelected()
        {
            // No reader schema is selected, so the rules have to come from the compiled-in
            // descriptor rather than schema text fetched from the registry.
            var config = new ProtobufSerializerConfig
            {
                AutoRegisterSchemas = true,
                ValidationRulesExecution = ValidationRulesExecution.AfterDomainRules
            };
            var serializer = new ProtobufSerializer<Example.ValidationPerson>(schemaRegistryClient,
                config, ValidatingRegistry());
            var headers = new Headers();
            var context = new SerializationContext(MessageComponentType.Value, testTopic, headers);

            var valid = new Example.ValidationPerson { Name = "Alice", FavoriteNumber = 30 };
            Assert.True(serializer.SerializeAsync(valid, context).Result.Length > 0);

            var invalid = new Example.ValidationPerson { Name = "", FavoriteNumber = 30 };
            var ex = Assert.Throws<AggregateException>(() =>
                serializer.SerializeAsync(invalid, context).Result);
            Assert.Contains("name must not be empty", ex.InnerException.Message);
        }

        [Fact]
        public void ProtobufReportsMessageLevelViolationsFromTheLocalDescriptor()
        {
            var config = new ProtobufSerializerConfig
            {
                AutoRegisterSchemas = true,
                ValidationRulesExecution = ValidationRulesExecution.BeforeDomainRules
            };
            var serializer = new ProtobufSerializer<Example.ValidationPerson>(schemaRegistryClient,
                config, ValidatingRegistry());
            var headers = new Headers();
            var context = new SerializationContext(MessageComponentType.Value, testTopic, headers);

            var invalid = new Example.ValidationPerson { Name = "forbidden", FavoriteNumber = -1 };
            var ex = Assert.Throws<AggregateException>(() =>
                serializer.SerializeAsync(invalid, context).Result);
            Assert.Contains("2 violations", ex.InnerException.Message);
            Assert.Contains("<root>: nameNotForbidden", ex.InnerException.Message);
            Assert.Contains("favorite_number: numberPositive", ex.InnerException.Message);
        }
        [Fact]
        public void ProtobufReadsBracketedListRulesFromTheLocalDescriptor()
        {
            // The bracketed list form compiles to two rules in the local descriptor; both are
            // read and enforced when the schema is auto-registered from that descriptor.
            var config = new ProtobufSerializerConfig
            {
                AutoRegisterSchemas = true,
                ValidationRulesExecution = ValidationRulesExecution.AfterDomainRules
            };
            var serializer = new ProtobufSerializer<Example.ValidationListForm>(schemaRegistryClient,
                config, ValidatingRegistry());
            var headers = new Headers();
            var context = new SerializationContext(MessageComponentType.Value, testTopic, headers);

            Assert.True(serializer
                .SerializeAsync(new Example.ValidationListForm { Id = "ord-1234" }, context)
                .Result.Length > 0);

            var ex = Assert.Throws<AggregateException>(() => serializer
                .SerializeAsync(new Example.ValidationListForm { Id = "x" }, context).Result);
            Assert.Contains("2 violations", ex.InnerException.Message);
            Assert.Contains("idPrefix", ex.InnerException.Message);
            Assert.Contains("id is too short", ex.InnerException.Message);
        }

        // A rule on an object-valued property is declared once and must fire once. The
        // property's schema and the schema the walk recurses into for it are the same
        // object, so a walk that read rules both in the property loop and on arrival
        // reports every such rule twice.
        private const string NestedRuleSchema = @"{
            ""type"": ""object"",
            ""confluent:rules"": [ { ""name"": ""rootRule"", ""expr"": ""has(this.child)"" } ],
            ""properties"": {
                ""child"": {
                    ""type"": ""object"",
                    ""confluent:rules"": [ { ""name"": ""childRule"", ""expr"": ""this.code == 'ok'"" } ],
                    ""properties"": {
                        ""code"": {
                            ""type"": ""string"",
                            ""confluent:rules"": [ { ""name"": ""codeRule"", ""expr"": ""size(this) > 0"" } ]
                        }
                    }
                }
            }
        }";

        private async Task<Dictionary<string, int>> CountRuleFirings(NestedRuleParent message)
        {
            var schema = new RegisteredSchema("topic-value", 1, 1, NestedRuleSchema, SchemaType.Json, null);
            store[NestedRuleSchema] = 1;
            subjectStore["topic-value"] = new List<RegisteredSchema> { schema };
            var config = new JsonSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = true,
                ValidationRulesExecution = ValidationRulesExecution.AfterDomainRules
            };
            var serializer = new JsonSerializer<NestedRuleParent>(
                schemaRegistryClient, config, null, ValidatingRegistry());

            string text = "";
            try
            {
                await serializer.SerializeAsync(message,
                    new SerializationContext(MessageComponentType.Value, testTopic));
            }
            catch (Exception e)
            {
                text = e.ToString();
            }

            var counts = new Dictionary<string, int>();
            foreach (var name in new[] { "rootRule", "childRule", "codeRule" })
            {
                int n = 0, i = 0;
                while ((i = text.IndexOf(name, i, StringComparison.Ordinal)) >= 0)
                {
                    n++;
                    i += name.Length;
                }

                if (n > 0)
                {
                    counts[name] = n;
                }
            }

            return counts;
        }

        [Fact]
        public async Task JsonEvaluatesEachRuleExactlyOnce()
        {
            // Both the object-valued property's rule and the scalar property's rule are violated.
            var counts = await CountRuleFirings(
                new NestedRuleParent { Child = new NestedRuleChild { Code = "" } });
            Assert.Equal(new Dictionary<string, int> { { "childRule", 1 }, { "codeRule", 1 } }, counts);
        }

        [Fact]
        public async Task JsonStillEvaluatesRootAndScalarRules()
        {
            // Proof that moving rule evaluation did not drop the root level.
            Assert.Equal(new Dictionary<string, int> { { "rootRule", 1 } },
                await CountRuleFirings(new NestedRuleParent()));
            Assert.Empty(await CountRuleFirings(
                new NestedRuleParent { Child = new NestedRuleChild { Code = "ok" } }));
        }


        // Skip-on-null is decided by whether the field tracks presence, not by whether it
        // sits in a oneof. proto3 hides the difference - an `optional` scalar there is a
        // synthetic oneof - but proto2 does not, so an unset proto2 `optional` is the shape
        // that tells the two predicates apart. It must not invoke the rule.
        [Fact]
        public void ProtobufSkipsUnsetFieldsThatTrackPresence()
        {
            var config = new ProtobufSerializerConfig
            {
                AutoRegisterSchemas = true,
                ValidationRulesExecution = ValidationRulesExecution.AfterDomainRules
            };
            var serializer = new ProtobufSerializer<Example.ValidationProto2Presence>(
                schemaRegistryClient, config, ValidatingRegistry());
            var context = new SerializationContext(MessageComponentType.Value, testTopic,
                new Headers());

            // opt is unset, so optRule must not run even though the empty string would fail it.
            var unset = new Example.ValidationProto2Presence { Req = "r" };
            Assert.True(serializer.SerializeAsync(unset, context).Result.Length > 0);

            // Explicitly set to the empty string, the field is present and the rule runs.
            var present = new Example.ValidationProto2Presence { Req = "r", Opt = "" };
            var ex = Assert.Throws<AggregateException>(() =>
                serializer.SerializeAsync(present, context).Result);
            Assert.Contains("optRule", ex.InnerException.Message);
        }

        // A field with no presence to report is never "unset": its rule always runs, and
        // asking HasValue about it throws. Guards against a fix that tests HasValue first.
        [Fact]
        public void ProtobufStillEvaluatesFieldsWithoutPresence()
        {
            var config = new ProtobufSerializerConfig
            {
                AutoRegisterSchemas = true,
                ValidationRulesExecution = ValidationRulesExecution.AfterDomainRules
            };
            var serializer = new ProtobufSerializer<Example.ValidationPresence>(
                schemaRegistryClient, config, ValidatingRegistry());
            var context = new SerializationContext(MessageComponentType.Value, testTopic,
                new Headers());

            // plain and many have no presence; plain is empty, so plainRule fires.
            var ex = Assert.Throws<AggregateException>(() =>
                serializer.SerializeAsync(new Example.ValidationPresence(), context).Result);
            Assert.Contains("plainRule", ex.InnerException.Message);
        }


        // The walk delivers an enum field to the executor as its number, so a rule on it can
        // pass. Before, every message with such a field was rejected because the rule could
        // not compile.
        [Fact]
        public void ProtobufEvaluatesRulesOnEnumFields()
        {
            var config = new ProtobufSerializerConfig
            {
                AutoRegisterSchemas = true,
                ValidationRulesExecution = ValidationRulesExecution.AfterDomainRules
            };
            var serializer = new ProtobufSerializer<Example.ValidationEnumHolder>(
                schemaRegistryClient, config, ValidatingRegistry());
            var context = new SerializationContext(MessageComponentType.Value, testTopic,
                new Headers());

            // isGreen is `this == 1`, and GREEN is 1.
            var green = new Example.ValidationEnumHolder { Color = Example.ValidationColor.Green };
            Assert.True(serializer.SerializeAsync(green, context).Result.Length > 0);

            var red = new Example.ValidationEnumHolder { Color = Example.ValidationColor.Red };
            var ex = Assert.Throws<AggregateException>(() =>
                serializer.SerializeAsync(red, context).Result);
            Assert.Contains("isGreen", ex.InnerException.Message);
        }


        // JsonObjectType is a flag set, so a schema declaring ["array","object"] carries both
        // the Array and Object flags. An object value used to enter the array branch, fail its
        // IList check and return - with the object's own property rules never visited. The
        // type is now narrowed to the kind the value actually is first, as Transform does.
        private const string MultiTypeSchema = @"{
            ""type"": [""array"", ""object""],
            ""properties"": {
                ""name"": {
                    ""type"": ""string"",
                    ""confluent:rules"": [
                        { ""name"": ""nameNotEmpty"", ""doc"": ""name must not be empty"", ""expr"": ""size(this) > 0"" }
                    ]
                }
            }
        }";

        private JsonSerializer<ValidationCustomer> MultiTypeSerializerFor()
        {
            var schema = new RegisteredSchema("topic-value", 1, 1, MultiTypeSchema, SchemaType.Json, null);
            store[MultiTypeSchema] = 1;
            subjectStore["topic-value"] = new List<RegisteredSchema> { schema };
            var config = new JsonSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = true,
                ValidationRulesExecution = ValidationRulesExecution.AfterDomainRules
            };
            return new JsonSerializer<ValidationCustomer>(schemaRegistryClient, config, null, ValidatingRegistry());
        }

        [Fact]
        public void JsonVisitsPropertiesOfAMultiTypeSchema()
        {
            var serializer = MultiTypeSerializerFor();
            var context = new SerializationContext(MessageComponentType.Value, testTopic);

            var ex = Assert.Throws<AggregateException>(() =>
                serializer.SerializeAsync(JsonCustomer("", 30), context).Result);
            Assert.Contains("name must not be empty", ex.InnerException.Message);
        }

        [Fact]
        public void JsonMultiTypeSchemaStillPassesAValidValue()
        {
            var serializer = MultiTypeSerializerFor();
            var context = new SerializationContext(MessageComponentType.Value, testTopic);
            Assert.True(serializer.SerializeAsync(JsonCustomer("Alice", 30), context).Result.Length > 0);
        }

    }
}
