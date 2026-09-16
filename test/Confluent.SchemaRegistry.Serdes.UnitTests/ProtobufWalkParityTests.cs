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
using System.Linq;
using System.Threading.Tasks;
using Confluent.SchemaRegistry.Serdes.Protobuf;
using Xunit;

namespace Confluent.SchemaRegistry.Serdes.UnitTests
{
    /// <summary>
    ///     The inline-validation walk and the field-transform walk have to reach the same
    ///     fields: both descend into a message-valued field with that field's own descriptor,
    ///     into every element of a repeated field, and into every value of a map. Walking any
    ///     of those against the containing descriptor looks their fields up in the wrong
    ///     message.
    /// </summary>
    public class ProtobufWalkParityTests
    {
        private const string ContainerSchema = @"syntax = ""proto3"";
import ""confluent/meta.proto"";

package example;

message ValidationItem {
    option (confluent.message_meta) = {
        rules: [{name: ""itemPositive"", expr: ""this.v > 0""}]
    };

    int32 v = 1;
    string label = 2 [(confluent.field_meta) = { tags: ""PII"" }];
}

message ValidationContainer {
    ValidationItem inner = 1;
    repeated ValidationItem items = 2;
    map<string, ValidationItem> labels = 3;
    repeated string tags = 4 [(confluent.field_meta) = { tags: ""PII"" }];
}";

        private static object ParseSchema() =>
            ProtobufUtils.Parse(ContainerSchema, null);

        private static Example.ValidationContainer NewContainer(int innerValue) =>
            new Example.ValidationContainer
            {
                Inner = new Example.ValidationItem { V = innerValue, Label = "inner" },
                Items = { new Example.ValidationItem { V = 1, Label = "first" } },
                Labels = { { "a", new Example.ValidationItem { V = 1, Label = "mapped" } } },
                Tags = { "t1", "t2" },
            };

        [Fact]
        public async Task ValidationWalkReachesEveryNestedMessage()
        {
            var executor = new CountingValidator();
            var violations = await ProtobufUtils.Validate(executor, ParseSchema(),
                NewContainer(-1), false);

            // itemPositive is declared on ValidationItem, so it fires once per item reached:
            // inner, the repeated element and the map value.
            Assert.Equal(new[] { "inner", "items[0]", "labels[\"a\"]" },
                violations.Select(v => v.FieldPath).ToArray());
            Assert.Equal(3, executor.Evaluations);
        }

        [Fact]
        public async Task TransformWalkReachesEveryNestedMessage()
        {
            var recorder = new RecordingTransform();
            var rule = new Rule("t", RuleKind.Transform, RuleMode.Write, "TEST",
                new HashSet<string> { "PII" }, null);
            var target = new Schema(ContainerSchema, SchemaType.Protobuf);
            var ctx = new RuleContext(null, null, target, "topic-value", "topic", null, false,
                RuleMode.Write, rule, 0, new List<Rule> { rule }, null);

            var result = await ProtobufUtils.Transform(ctx, ParseSchema(), NewContainer(1),
                recorder);

            // The PII-tagged label under inner, the repeated element and the map value, plus
            // the two tagged repeated scalars.
            Assert.Equal(new[] { "label", "label", "label", "tags", "tags" },
                recorder.Visited.ToArray());

            var container = Assert.IsType<Example.ValidationContainer>(result);
            Assert.Equal("inner-suffix", container.Inner.Label);
            Assert.Equal("first-suffix", container.Items[0].Label);
            Assert.Equal("mapped-suffix", container.Labels["a"].Label);
            Assert.Equal(new[] { "t1-suffix", "t2-suffix" }, container.Tags.ToArray());
        }

        [Fact]
        public async Task TransformWalkLeavesAbsentFieldsAbsent()
        {
            // Writing a transformed default back would materialize the field: an absent
            // message would become present, carrying a transformed default.
            var recorder = new RecordingTransform();
            var rule = new Rule("t", RuleKind.Transform, RuleMode.Write, "TEST",
                new HashSet<string> { "PII" }, null);
            var target = new Schema(ContainerSchema, SchemaType.Protobuf);
            var ctx = new RuleContext(null, null, target, "topic-value", "topic", null, false,
                RuleMode.Write, rule, 0, new List<Rule> { rule }, null);

            // inner is absent; only the tagged repeated scalars are present.
            var container = new Example.ValidationContainer { Tags = { "t1" } };
            var result = await ProtobufUtils.Transform(ctx, ParseSchema(), container, recorder);

            var transformed = Assert.IsType<Example.ValidationContainer>(result);
            Assert.Null(transformed.Inner);
            Assert.Equal(new[] { "t1-suffix" }, transformed.Tags.ToArray());
        }

        /// <summary>
        ///     Protobuf identifies a field by its number, and renaming a field at the same
        ///     number is a compatible change, so with use.latest.version the registered
        ///     schema's name for a field can differ from the message's. Resolving the
        ///     schema-side field by name would find nothing and silently skip its rules and
        ///     tags - here, leaving a tagged field untransformed.
        /// </summary>
        [Fact]
        public async Task WalksResolveRenamedFieldsByNumber()
        {
            // The registered schema calls field 2 "renamed" and tags it; the generated class
            // calls it "label".
            const string renamedSchema = @"syntax = ""proto3"";
import ""confluent/meta.proto"";

package example;

message ValidationItem {
    option (confluent.message_meta) = {
        rules: [{name: ""itemPositive"", expr: ""this.v > 0""}]
    };

    int32 v = 1;
    string renamed = 2 [(confluent.field_meta) = { tags: ""PII"" }];
}";
            object schema = ProtobufUtils.Parse(renamedSchema, null);
            var item = new Example.ValidationItem { V = 1, Label = "secret" };

            var recorder = new RecordingTransform();
            var rule = new Rule("t", RuleKind.Transform, RuleMode.Write, "TEST",
                new HashSet<string> { "PII" }, null);
            var target = new Schema(renamedSchema, SchemaType.Protobuf);
            var ctx = new RuleContext(null, null, target, "topic-value", "topic", null, false,
                RuleMode.Write, rule, 0, new List<Rule> { rule }, null);

            var result = await ProtobufUtils.Transform(ctx, schema, item, recorder);

            var transformed = Assert.IsType<Example.ValidationItem>(result);
            Assert.Equal("secret-suffix", transformed.Label);
            // The name reported to the rule is the registered schema's.
            Assert.Equal(new[] { "renamed" }, recorder.Visited.ToArray());
        }

        private class CountingValidator : IValidationRuleExecutor
        {
            public int Evaluations { get; private set; }

            public Task<object> Execute(ValidationRule rule, object schema, object message)
            {
                Evaluations++;
                return Task.FromResult<object>(false);
            }
        }

        private class RecordingTransform : IFieldTransform
        {
            public List<string> Visited { get; } = new List<string>();

            public void Init(RuleContext ctx)
            {
            }

            public void Dispose()
            {
            }

            public Task<object> Transform(RuleContext ctx, RuleContext.FieldContext fieldCtx,
                object fieldValue)
            {
                Visited.Add(fieldCtx.Name);
                if (fieldValue is string text)
                {
                    return Task.FromResult<object>(text + "-suffix");
                }

                return Task.FromResult(fieldValue);
            }
        }
    }
}
