// Copyright 2016-2019 Confluent Inc.
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

#pragma warning disable xUnit1026

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Confluent.Kafka.Admin;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Tests the validation of input parameter to guard against uncaught segfaults.
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public void AdminClient_NullReferenceChecks(string bootstrapServers)
        {
            LogToFile("start AdminClient_NullReferenceChecks");
            var topicName1 = Guid.NewGuid().ToString();
            string nullTopic = null;

            // test creating a null topic throws a related exception
            using (var producer = new ProducerBuilder<Null, Null>(new ProducerConfig { BootstrapServers = bootstrapServers }).Build())
            using (var adminClient = new DependentAdminClientBuilder(producer.Handle).Build())
            {
                try
                {
                    adminClient.CreateTopicsAsync(new TopicSpecification[] { new TopicSpecification { Name = nullTopic, NumPartitions = 1, ReplicationFactor = 1 } }).Wait();
                    Assert.True(false, "Expected exception.");
                }
                catch (KafkaException ex)
                {
                    Assert.Contains("topic", ex.Message.ToLower());
                }
            }

            // test creating a partition with null topic throws exception
            using (var producer = new ProducerBuilder<Null, Null>(new ProducerConfig { BootstrapServers = bootstrapServers }).Build())
            using (var adminClient = new DependentAdminClientBuilder(producer.Handle).Build())
            {
                try
                {
                    adminClient.CreateTopicsAsync(new TopicSpecification[] { new TopicSpecification { Name = topicName1, NumPartitions = 1, ReplicationFactor = 1 } }).Wait();
                    adminClient.CreatePartitionsAsync(new List<PartitionsSpecification> { new PartitionsSpecification { Topic = nullTopic, IncreaseTo = 2 } }).Wait();
                    Assert.True(false, "Expected exception.");
                }
                catch (NullReferenceException ex)
                {
                    Assert.Contains("topic", ex.Message.ToLower());
                }
            }

            // test adding a null list of brokers throws null reference exception.
            using (var producer = new ProducerBuilder<Null, Null>(new ProducerConfig { BootstrapServers = bootstrapServers }).Build())
            using (var adminClient = new DependentAdminClientBuilder(producer.Handle).Build())
            {
                try
                {
                    adminClient.AddBrokers(null);
                    Assert.True(false, "Expected exception.");
                }
                catch (NullReferenceException ex)
                {
                    Assert.Contains("broker", ex.Message.ToLower());
                }
            }

            // test retrieving metadata for a null topic
            using (var producer = new ProducerBuilder<Null, Null>(new ProducerConfig { BootstrapServers = bootstrapServers }).Build())
            using (var adminClient = new DependentAdminClientBuilder(producer.Handle).Build())
            {
                try
                {
                    adminClient.GetMetadata(null, TimeSpan.FromSeconds(10));
                    Assert.True(false, "Expected exception.");
                }
                catch (ArgumentNullException ex)
                {
                    Assert.Contains("value cannot be null", ex.Message.ToLower());
                }
            }

            // This test-case completing is the test case
            // Used to cause a seg-fault.
            using (var producer = new ProducerBuilder<Null, Null>(new ProducerConfig { BootstrapServers = bootstrapServers }).Build())
            using (var adminClient = new DependentAdminClientBuilder(producer.Handle).Build())
            {
                adminClient.DeleteTopicsAsync(new List<string> { topicName1, nullTopic });
                Assert.True(true);
            }

            // Asserts existing ListGroup behavior - null group is valid option
            using (var producer = new ProducerBuilder<Null, Null>(new ProducerConfig { BootstrapServers = bootstrapServers }).Build())
            using (var adminClient = new DependentAdminClientBuilder(producer.Handle).Build())
            {
                adminClient.ListGroup(null, TimeSpan.FromSeconds(10));
                Assert.True(true);
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   AdminClient_NullReferenceChecks");
        }
    }
}
