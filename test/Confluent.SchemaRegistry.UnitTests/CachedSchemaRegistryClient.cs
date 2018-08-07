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
using Xunit;


namespace Confluent.SchemaRegistry.UnitTests
{
    public class CachedSchemaRegistryClientTests
    {
        [Fact]
        public void NullConfig()
        {
            Assert.Throws<ArgumentNullException>(() => new CachedSchemaRegistryClient(null));
        }

        [Fact]
        public void NoUrls()
        {
            var config = new Dictionary<string, object>();
            Assert.Throws<ArgumentException>(() => new CachedSchemaRegistryClient(config));
        }
        
        [Fact]
        public void InvalidSubjectNameStrategy()
        {
            var config = new Dictionary<string, object>
            {
                { "schema.registry.url", "irrelevanthost:8081" },
                { "schema.registry.subject.name.strategy", "bad_value" }
            };
            Assert.Throws<ArgumentException>(() => new CachedSchemaRegistryClient(config));
        }

        [Fact]
        public void ConstructKeySubjectName()
        {
            var config = new Dictionary<string, object>
            {
                { "schema.registry.url", "irrelevanthost:8081" }
            };

            CachedSchemaRegistryClient src = new CachedSchemaRegistryClient(config);
            Assert.Equal("mytopic-key", src.ConstructKeySubjectName("mytopic", "myschemaname"));
            
            var configTopicName = new Dictionary<string, object>
            {
                { "schema.registry.url", "irrelevanthost:8081" },
                { "schema.registry.subject.name.strategy", "topic_name_strategy" }
            };

            CachedSchemaRegistryClient srcTopicName = new CachedSchemaRegistryClient(configTopicName);
            Assert.Equal("mytopic-key", srcTopicName.ConstructKeySubjectName("mytopic", "myschemaname"));
            
            var configRecordName = new Dictionary<string, object>
            {
                { "schema.registry.url", "irrelevanthost:8081" },
                { "schema.registry.subject.name.strategy", "record_name_strategy" }
            };

            CachedSchemaRegistryClient srcRecordName = new CachedSchemaRegistryClient(configRecordName);
            Assert.Equal("myschemaname", srcRecordName.ConstructKeySubjectName("mytopic", "myschemaname"));
            
            var configTopicRecordName = new Dictionary<string, object>
            {
                { "schema.registry.url", "irrelevanthost:8081" },
                { "schema.registry.subject.name.strategy", "topic_record_name_strategy" }
            };

            CachedSchemaRegistryClient srcTopicRecordName = new CachedSchemaRegistryClient(configTopicRecordName);
            Assert.Equal("mytopic-myschemaname", srcTopicRecordName.ConstructKeySubjectName("mytopic", "myschemaname"));
        }

        [Fact]
        public void ConstructValueSubjectName()
        {
            var config = new Dictionary<string, object>
            {
                { "schema.registry.url", "irrelevanthost:8081" }
            };

            CachedSchemaRegistryClient src = new CachedSchemaRegistryClient(config);
            Assert.Equal("mytopic-value", src.ConstructValueSubjectName("mytopic", "myschemaname"));
            
            var configTopicName = new Dictionary<string, object>
            {
                { "schema.registry.url", "irrelevanthost:8081" },
                { "schema.registry.subject.name.strategy", "topic_name_strategy" }
            };

            CachedSchemaRegistryClient srcTopicName = new CachedSchemaRegistryClient(configTopicName);
            Assert.Equal("mytopic-value", srcTopicName.ConstructValueSubjectName("mytopic", "myschemaname"));
            
            var configRecordName = new Dictionary<string, object>
            {
                { "schema.registry.url", "irrelevanthost:8081" },
                { "schema.registry.subject.name.strategy", "record_name_strategy" }
            };

            CachedSchemaRegistryClient srcRecordName = new CachedSchemaRegistryClient(configRecordName);
            Assert.Equal("myschemaname", srcRecordName.ConstructValueSubjectName("mytopic", "myschemaname"));
            
            var configTopicRecordName = new Dictionary<string, object>
            {
                { "schema.registry.url", "irrelevanthost:8081" },
                { "schema.registry.subject.name.strategy", "topic_record_name_strategy" }
            };

            CachedSchemaRegistryClient srcTopicRecordName = new CachedSchemaRegistryClient(configTopicRecordName);
            Assert.Equal("mytopic-myschemaname", srcTopicRecordName.ConstructValueSubjectName("mytopic", "myschemaname"));
        }
    }
}
