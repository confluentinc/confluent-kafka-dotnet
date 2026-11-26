// Copyright 2016-2017 Confluent Inc.
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
using System.Text;
using System.Collections.Generic;
using System.Threading;


namespace Confluent.Kafka.UnitTests
{
    public class ProducerTests
    {
        [Fact]
        public void Constuctor()
        {
            // Throw exception if a config value is null and ensure that exception mentions the
            // respective config key.
            var configWithNullValue = new ProducerConfig();
            configWithNullValue.Set("sasl.password", null);
            var e = Assert.Throws<ArgumentNullException>(() => { var c = new ProducerBuilder<byte[], byte[]>(configWithNullValue).Build(); });
            Assert.Contains("sasl.password", e.Message);
        }

        // librdkafka can invoke the log callback before rd_kafka_new returns, i.e. before the
        // client has a native handle. IClient.Name must still work in that window (log handlers
        // commonly read it) rather than throwing or returning null. Logs raised during Build()
        // come from background threads, so build repeatedly until some have been observed.
        // No broker is needed: the bootstrap address refuses connections.
        internal static void VerifyNameDuringBuild(string clientId, Func<Action<IClient, LogMessage>, IDisposable> build)
        {
            int building = 0, duringBuild = 0;
            string problem = null;

            Action<IClient, LogMessage> observe = (client, logMessage) =>
            {
                if (Volatile.Read(ref building) != 1) { return; }
                try
                {
                    var name = client.Name;
                    // Name must be the expected client name, and match the name librdkafka put on the log message.
                    if (name != null && name.StartsWith(clientId) && name == logMessage.Name) { Interlocked.Increment(ref duringBuild); }
                    else { Interlocked.CompareExchange(ref problem, "Unexpected name: " + (name ?? "null") + " (log message name: " + logMessage.Name + ")", null); }
                }
                catch (Exception e) { Interlocked.CompareExchange(ref problem, e.GetType().Name + ": " + e.Message, null); }
            };

            for (int i = 0; i < 50 && duringBuild == 0; ++i)
            {
                Volatile.Write(ref building, 1);
                IDisposable client;
                try { client = build(observe); }
                finally { Volatile.Write(ref building, 0); }
                client.Dispose();
            }

            Assert.Null(problem);
            Assert.True(duringBuild > 0, "No log callback was observed during Build().");
        }

        [Fact]
        public void NameAvailableDuringConstruction()
            => VerifyNameDuringBuild("producer-name-test", onLog =>
                new ProducerBuilder<byte[], byte[]>(new ProducerConfig
                {
                    BootstrapServers = "localhost:1", ClientId = "producer-name-test", Debug = "all"
                })
                .SetLogHandler((c, m) => onLog(c, m))
                .Build());

        [Fact]
        public void AdminClient_NameAvailableDuringConstruction()
            => VerifyNameDuringBuild("admin-name-test", onLog =>
                new AdminClientBuilder(new AdminClientConfig
                {
                    BootstrapServers = "localhost:1", ClientId = "admin-name-test", Debug = "all"
                })
                .SetLogHandler((c, m) => onLog(c, m))
                .Build());

        [Fact]
        public void DependentProducer_NameMatchesOwner()
        {
            string logName = null;
            var logged = new ManualResetEventSlim();

            using (var owner = new ProducerBuilder<byte[], byte[]>(new ProducerConfig
                { BootstrapServers = "localhost:1", ClientId = "dependent-name-test", Debug = "all" })
                .SetLogHandler((c, m) => { Interlocked.CompareExchange(ref logName, m.Name, null); logged.Set(); })
                .Build())
            using (var dependent = new DependentProducerBuilder<string, string>(owner.Handle).Build())
            {
                Assert.True(logged.Wait(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken), "No log message was received.");

                // Name must be the expected client name, and match the name on the log message.
                Assert.StartsWith("dependent-name-test", dependent.Name);
                Assert.Equal(logName, owner.Name);
                Assert.Equal(logName, dependent.Name);
            }
        }
    }
}
