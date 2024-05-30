﻿// Copyright 2018 Confluent Inc.
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

using Moq;
using Xunit;
using System.Collections.Generic;
using System.Linq;
using Avro.Specific;
using Confluent.Kafka;
using Confluent.Kafka.Examples.AvroSpecific;
using System;
using Avro.Generic;
using Confluent.SchemaRegistry.Encryption;
using Confluent.SchemaRegistry.Rules;

namespace Confluent.SchemaRegistry.Serdes.UnitTests
{
    public class BaseSerializeDeserializeTests
    {
        protected ISchemaRegistryClient schemaRegistryClient;
        protected IDekRegistryClient dekRegistryClient;
        protected string testTopic;
        protected IDictionary<string, int> store = new Dictionary<string, int>();
        protected IDictionary<string, List<RegisteredSchema>> subjectStore = new Dictionary<string, List<RegisteredSchema>>();
        protected IDictionary<KekId, RegisteredKek> kekStore = new Dictionary<KekId, RegisteredKek>();
        protected IDictionary<DekId, RegisteredDek> dekStore = new Dictionary<DekId, RegisteredDek>();

        public BaseSerializeDeserializeTests()
        {
            testTopic = "topic";
            var schemaRegistryMock = new Mock<ISchemaRegistryClient>();
            schemaRegistryMock.Setup(x => x.ConstructValueSubjectName(testTopic, It.IsAny<string>())).Returns($"{testTopic}-value");
            schemaRegistryMock.Setup(x => x.RegisterSchemaAsync("topic-value", It.IsAny<string>(), It.IsAny<bool>())).ReturnsAsync(
                (string topic, string schema, bool normalize) => store.TryGetValue(schema, out int id) ? id : store[schema] = store.Count + 1
            );
            schemaRegistryMock.Setup(x => x.GetSchemaAsync(It.IsAny<int>(), It.IsAny<string>())).ReturnsAsync(
                (int id, string format) =>
                {
                    try
                    {
                        // First try subjectStore
                        return subjectStore.Values.SelectMany(x => x.Where(x => x.Id == id)).First();
                    }
                    catch (InvalidOperationException e)
                    {
                        // Next try store
                        return new Schema(store.Where(x => x.Value == id).First().Key, null, SchemaType.Avro);
                    }
                });
            schemaRegistryMock.Setup(x => x.GetRegisteredSchemaAsync(It.IsAny<string>(), It.IsAny<int>())).ReturnsAsync(
                (string subject, int version) => subjectStore[subject].First(x => x.Version == version)
            );
            schemaRegistryMock.Setup(x => x.GetLatestSchemaAsync(It.IsAny<string>())).ReturnsAsync(
                (string subject) => subjectStore[subject].Last()
            );
            schemaRegistryMock.Setup(x => x.GetLatestWithMetadataAsync(It.IsAny<string>(), It.IsAny<IDictionary<string, string>>(), It.IsAny<bool>())).ReturnsAsync(
                (string subject, IDictionary<string, string> metadata, bool ignoreDeleted) =>
                {
                    return subjectStore[subject].First(x =>
                        x.Metadata != null 
                        && x.Metadata.Properties != null 
                        && metadata.Keys.All(k => x.Metadata.Properties.ContainsKey(k) && x.Metadata.Properties[k] == metadata[k])
                    );
                }
            );
            schemaRegistryClient = schemaRegistryMock.Object;
            
            var dekRegistryMock = new Mock<IDekRegistryClient>();
            dekRegistryMock.Setup(x => x.CreateKekAsync(It.IsAny<Kek>())).ReturnsAsync(
                (Kek kek) =>
                {
                    var kekId = new KekId(kek.Name, false);
                    return kekStore.TryGetValue(kekId, out RegisteredKek registeredKek)
                        ? registeredKek
                        : kekStore[kekId] = new RegisteredKek
                        {
                            Name = kek.Name,
                            KmsType = kek.KmsType,
                            KmsKeyId = kek.KmsKeyId,
                            KmsProps = kek.KmsProps,
                            Doc = kek.Doc,
                            Shared = kek.Shared,
                            Deleted = false,
                            Timestamp = DateTimeOffset.Now.ToUnixTimeMilliseconds()
                        };
                });
            dekRegistryMock.Setup(x => x.GetKekAsync(It.IsAny<string>(), It.IsAny<bool>())).ReturnsAsync(
                (string name, bool ignoreDeletedKeks) =>
                {
                    var kekId = new KekId(name, false);
                    return kekStore.TryGetValue(kekId, out RegisteredKek registeredKek) ? registeredKek : null;
                });
            dekRegistryMock.Setup(x => x.CreateDekAsync(It.IsAny<string>(), It.IsAny<Dek>())).ReturnsAsync(
                (string kekName, Dek dek) =>
                {
                    int version = dek.Version ?? 1;
                    var dekId = new DekId(kekName, dek.Subject, version, dek.Algorithm, false);
                    return dekStore.TryGetValue(dekId, out RegisteredDek registeredDek)
                        ? registeredDek
                        : dekStore[dekId] = new RegisteredDek
                        {
                            KekName = kekName,
                            Subject = dek.Subject,
                            Version = version,
                            Algorithm = dek.Algorithm,
                            EncryptedKeyMaterial = dek.EncryptedKeyMaterial,
                            Deleted = false,
                            Timestamp = DateTimeOffset.Now.ToUnixTimeMilliseconds()
                        };
                });
            dekRegistryMock.Setup(x => x.GetDekAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<DekFormat>(), It.IsAny<bool>())).ReturnsAsync(
                (string kekName, string subject, DekFormat? algorithm, bool ignoreDeletedKeks) =>
                {
                    var dekId = new DekId(kekName, subject, 1, algorithm, false);
                    return dekStore.TryGetValue(dekId, out RegisteredDek registeredDek) ? registeredDek : null;
                });
            dekRegistryMock.Setup(x => x.GetDekVersionAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<int>(), It.IsAny<DekFormat>(), It.IsAny<bool>())).ReturnsAsync(
                (string kekName, string subject, int version, DekFormat? algorithm, bool ignoreDeletedKeks) =>
                {
                    var dekId = new DekId(kekName, subject, version, algorithm, false);
                    return dekStore.TryGetValue(dekId, out RegisteredDek registeredDek) ? registeredDek : null;
                });
            dekRegistryClient = dekRegistryMock.Object;
            
            // Register kms drivers
            LocalKmsDriver.Register();
            CelExecutor.Register();
            CelFieldExecutor.Register();
            JsonataExecutor.Register();
        }
    }
}
