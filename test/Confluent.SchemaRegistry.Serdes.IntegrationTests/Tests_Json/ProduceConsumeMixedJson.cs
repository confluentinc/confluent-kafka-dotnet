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

using Xunit;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Newtonsoft.Json;


namespace Confluent.SchemaRegistry.Serdes.IntegrationTests
{
    public static partial class Tests
    {
        // inspired by: https://github.com/RicoSuter/NJsonSchema#njsonschema-usage
        public class PersonPoco
        {
            [Required] // via System.ComponentModel.DataAnnotations
            public string FirstName { get; set; }

            public string MiddleName { get; set; }

            [JsonRequired] // via Newtonsoft.Json
            public string LastName { get; set; }

            public GenderEnum Gender { get; set; }

            [Range(2, 5)] // via System.ComponentModel.DataAnnotations
            public int NumberWithRange { get; set; }

            public DateTime Birthday { get; set; }

            public CompanyPoco Company { get; set; }

            public List<CarPoco> Cars { get; set; }
        }

        public enum GenderEnum
        {
            Male,
            Female
        }

        public class CarPoco
        {
            public string Name { get; set; }

            public CompanyPoco Manufacturer { get; set; }
        }

        public class CompanyPoco
        {
            public string Name { get; set; }
        }


        /// <summary>
        ///     Test various things
        /// </summary>
        [Theory, MemberData(nameof(TestParameters))]
        public static void ProduceConsumeMixedJson(string bootstrapServers, string schemaRegistryServers)
        {
            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
            var schemaRegistryConfig = new SchemaRegistryConfig { Url = schemaRegistryServers };

            using (var topic = new TemporaryTopic(bootstrapServers, 1))
            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var producer =
                new ProducerBuilder<string, PersonPoco>(producerConfig)
                    .SetValueSerializer(new JsonSerializer<PersonPoco>(schemaRegistry))
                    .Build())
            {
                // test that a System.ComponentModel.DataAnnotations constraint is effective.
                {
                    var p = new PersonPoco
                    {
                        FirstName = "Test",
                        LastName = "User",
                        NumberWithRange = 7 // range should be between 2 and 5.
                    };
                    Assert.Throws<ProduceException<string, PersonPoco>>(() => {
                        try
                        {
                            producer.ProduceAsync(topic.Name, new Message<string, PersonPoco> { Key = "test1", Value = p }).Wait();
                        }
                        catch (AggregateException ax)
                        {
                            // what would be thrown if the call was awaited.
                            throw ax.InnerException;
                        }
                    });
                }

                // test that a Newtonsoft.Json constraint is effective.
                {
                    var p = new PersonPoco
                    {
                        FirstName = "Test",
                        // Omit LastName
                        NumberWithRange = 3
                    };
                    Assert.Throws<AggregateException>(() => {
                        producer.ProduceAsync(topic.Name, new Message<string, PersonPoco> { Key = "test1", Value = p }).Wait();
                    });
                }

                // test all fields valid.
                {
                    var p = new PersonPoco
                    {
                        FirstName = "A",
                        MiddleName = "Test",
                        LastName = "User",
                        Gender = GenderEnum.Male,
                        NumberWithRange = 3,
                        Birthday = new DateTime(2010, 11, 1),
                        Company = new CompanyPoco
                        {
                            Name = "Confluent"
                        },
                        Cars = new List<CarPoco>
                        {
                            new CarPoco
                            {
                                Name = "Volvo",
                                Manufacturer = new CompanyPoco
                                {
                                    Name = "Confluent"
                                }
                            }
                        }
                    };
                    producer.ProduceAsync(topic.Name, new Message<string, PersonPoco> { Key = "test1", Value = p }).Wait();

                    var schema = schemaRegistry.GetLatestSchemaAsync(SubjectNameStrategy.Topic.ConstructValueSubjectName(topic.Name, null)).Result.SchemaString;

                    var consumerConfig = new ConsumerConfig
                    {
                        BootstrapServers = bootstrapServers,
                        GroupId = Guid.NewGuid().ToString(),
                        AutoOffsetReset = AutoOffsetReset.Earliest
                    };

                    using (var consumer =
                        new ConsumerBuilder<string, PersonPoco>(consumerConfig)
                            .SetValueDeserializer(new JsonDeserializer<PersonPoco>().AsSyncOverAsync())
                            .Build())
                    {
                        consumer.Subscribe(topic.Name);
                        var cr = consumer.Consume();
                        Assert.Equal(p.FirstName, cr.Message.Value.FirstName);
                        Assert.Equal(p.MiddleName, cr.Message.Value.MiddleName);
                        Assert.Equal(p.LastName, cr.Message.Value.LastName);
                        Assert.Equal(p.Gender, cr.Message.Value.Gender);
                        Assert.Equal(p.NumberWithRange, cr.Message.Value.NumberWithRange);
                        Assert.Equal(p.Birthday, cr.Message.Value.Birthday);
                        Assert.Equal(p.Company.Name, cr.Message.Value.Company.Name);
                        Assert.Single(cr.Message.Value.Cars);
                        Assert.Equal(cr.Message.Value.Cars[0].Manufacturer.Name, p.Cars[0].Manufacturer.Name);
                        Assert.Equal(cr.Message.Value.Cars[0].Name, p.Cars[0].Name);
                    }
                }
            }
        }
    }
}
