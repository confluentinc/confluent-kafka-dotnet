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

using System;
using Xunit;


namespace Confluent.SchemaRegistry.IntegrationTests
{
    public static partial class Tests
    {
        [Theory, MemberData(nameof(SchemaRegistryParameters))]
        public static void ProtobufWithReferences(Config config)
        {
            // FIXME : The Tests always Fails !!
            var srInitial = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = config.Server });
            var sr = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = config.Server });
            var testSchemaBase64 = Confluent.Kafka.Examples.Protobuf.Person.Descriptor.File.SerializedData.ToBase64();
            var topicName = Guid.NewGuid().ToString();
            var subjectInitial = SubjectNameStrategy.Topic.ConstructValueSubjectName(topicName, null);
            var subject = SubjectNameStrategy.Topic.ConstructValueSubjectName(topicName + "2", null);

            // check that registering a base64 protobuf schema works.
            var id1 = srInitial.RegisterSchemaAsync(subjectInitial, new Schema(testSchemaBase64, SchemaType.Protobuf)).Result;

            var sc = srInitial.GetSchemaAsync(id1).Result;
        }
    }
}