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

using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System;
using System.Threading.Tasks;
using System.Threading;


namespace Web
{
    /// <summary>
    ///     A simple example demonstrating how to set up a Kafka consumer as an
    ///     IHostedService.
    /// </summary>
    public class RequestTimeConsumer : IHostedService, IDisposable
    {
        string topic;
        ConsumerConfig consumerConfig;
        IConsumer<string, long> kafkaConsumer;
        Thread pollThread;
        CancellationTokenSource cancellationTokenSource;

        public RequestTimeConsumer(IConfiguration config)
        {
            consumerConfig = new ConsumerConfig();
            config.GetSection("Kafka:ConsumerSettings").Bind(consumerConfig);
            this.topic = config.GetValue<string>("Kafka:RequestTimeTopic");
        }

        public void Dispose()
        {
            this.kafkaConsumer.Close(); // Commit offsets and leave the group cleanly.
            this.kafkaConsumer.Dispose();
        }

        private void consumerLoop()
        {
            kafkaConsumer.Subscribe(this.topic);

            while (true)
            {
                try
                {
                    var cr = this.kafkaConsumer.Consume(this.cancellationTokenSource.Token);

                    // Handle message...
                    Console.WriteLine($"{cr.Message.Key}: {cr.Message.Value}ms");
                }
                catch (TaskCanceledException)
                {
                    // StopAsync called.
                    break;
                }
                catch (ConsumeException e)
                {
                    // Consumer errors should generally be ignored (or logged) unless fatal.
                    Console.WriteLine($"Consume error: {e.Error.Reason}");

                    if (e.Error.IsFatal)
                    {
                        // https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md#fatal-consumer-errors
                        break;
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Unexpected error: {e}");
                    break;
                }
            }
        }

        public Task StartAsync(CancellationToken _cancellationToken)
        {
            // The passed in cancellationToken is to allow for cancellation of the StartAsync method.
            // Our StartAsync implementation executes quickly - has no blocking or async calls, so
            // this is not needed.

            // Create a cancellation token source to allow the consumer poll loop to be cancelled
            // by the StopAsync method.
            this.cancellationTokenSource = new CancellationTokenSource();
            this.kafkaConsumer = new ConsumerBuilder<string, long>(consumerConfig).Build();
            this.pollThread = new Thread(consumerLoop);
            this.pollThread.Start();
            return Task.CompletedTask;
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            this.cancellationTokenSource.Cancel();

            // Async methods should never block, so block waiting for the poll loop to finish on another
            // thread and await completion of that.
            await Task.Run(() => { this.pollThread.Join(); }, cancellationToken);
        }
    }
}
