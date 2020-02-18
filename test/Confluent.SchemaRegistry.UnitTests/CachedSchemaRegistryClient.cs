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
            var config = new SchemaRegistryConfig();
            Assert.Throws<ArgumentException>(() => new CachedSchemaRegistryClient(config));
        }

        [Fact]
        [Obsolete]
        public void InvalidSubjectNameStrategy()
        {
            var config = new SchemaRegistryConfig { Url = "irrelevanthost:8081" };
            config.Set(SchemaRegistryConfig.PropertyNames.SchemaRegistryKeySubjectNameStrategy, "bad_value");
            Assert.Throws<ArgumentException>(() => new CachedSchemaRegistryClient(config));
        }

        [Fact]
        [Obsolete]
        public void ConstructKeySubjectName_Topic1()
        {
            var config = new SchemaRegistryConfig { Url = "irrelevanthost:8081" };
            var src = new CachedSchemaRegistryClient(config);
            Assert.Equal("mytopic-key", src.ConstructKeySubjectName("mytopic", "myschemaname"));
        }

        [Fact]
        [Obsolete]
        public void ConstructKeySubjectName_Topic2()
        {
            var config = new SchemaRegistryConfig
            {
                Url = "irrelevanthost:8081",
                KeySubjectNameStrategy = SubjectNameStrategy.Topic
            };
            var src = new CachedSchemaRegistryClient(config);
            Assert.Equal("mytopic-key", src.ConstructKeySubjectName("mytopic", "myschemaname"));
        }

        [Fact]
        [Obsolete]
        public void ConstructKeySubjectName_Record()
        {
            var config = new SchemaRegistryConfig
            {
                Url = "irrelevanthost:8081",
                KeySubjectNameStrategy = SubjectNameStrategy.Record
            };
            var src = new CachedSchemaRegistryClient(config);
            Assert.Equal("myschemaname", src.ConstructKeySubjectName("mytopic", "myschemaname"));
        }

        [Fact]
        [Obsolete]
        public void ConstructKeySubjectName_TopicRecord()
        {
            var config = new SchemaRegistryConfig
            {
                Url = "irrelevanthost:8081",
                KeySubjectNameStrategy = SubjectNameStrategy.TopicRecord
            };
            var src = new CachedSchemaRegistryClient(config);
            Assert.Equal("mytopic-myschemaname", src.ConstructKeySubjectName("mytopic", "myschemaname"));
        }

        [Fact]
        [Obsolete]
        public void ConstructValueSubjectName_Topic1()
        {
            var config = new SchemaRegistryConfig { Url = "irrelevanthost:8081" };
            var src = new CachedSchemaRegistryClient(config);
            Assert.Equal("mytopic-value", src.ConstructValueSubjectName("mytopic", "myschemaname"));
        }

        [Fact]
        [Obsolete]
        public void ConstructValueSubjectName_Topic2()
        {
            var config = new SchemaRegistryConfig
            {
                Url = "irrelevanthost:8081",
                ValueSubjectNameStrategy = SubjectNameStrategy.Topic
            };
            var src = new CachedSchemaRegistryClient(config);
            Assert.Equal("mytopic-value", src.ConstructValueSubjectName("mytopic", "myschemaname"));
        }

        [Fact]
        [Obsolete]
        public void ConstructValueSubjectName_Record()
        {
            var config = new SchemaRegistryConfig
            {
                Url = "irrelevanthost:8081",
                ValueSubjectNameStrategy = SubjectNameStrategy.Record
            };
            var src = new CachedSchemaRegistryClient(config);
            Assert.Equal("myschemaname", src.ConstructValueSubjectName("mytopic", "myschemaname"));
        }

        [Fact]
        [Obsolete]
        public void ConstructValueSubjectName_TopicRecord()
        {
            var config = new SchemaRegistryConfig
            {
                Url = "irrelevanthost:8081",
                ValueSubjectNameStrategy = SubjectNameStrategy.TopicRecord
            };
            var src = new CachedSchemaRegistryClient(config);
            Assert.Equal("mytopic-myschemaname", src.ConstructValueSubjectName("mytopic", "myschemaname"));
        }
    }
}
