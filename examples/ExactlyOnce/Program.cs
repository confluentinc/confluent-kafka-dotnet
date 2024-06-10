// Copyright 2022 Confluent Inc.
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

using Confluent.Kafka.Admin;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Linq;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.IO;


namespace Confluent.Kafka.Examples.ExactlyOnce
{
    public class Program
    {
        private static readonly ILogger logger = Utils.LoggerFactory.CreateLogger<Program>();

        public static async Task Main(string[] args)
        {
            var cts = new CancellationTokenSource();
            var completedCancellationTokenSource = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };
            AppDomain.CurrentDomain.ProcessExit += (_, e) =>
            {
                cts.Cancel();
                completedCancellationTokenSource.Token.WaitHandle.WaitOne();
            };

            if (args.Length == 0 ||
                (args[0] == "print_words" && args.Length != 1) ||
                args[0] != "print_words" && args.Length != 2)
            {
                Console.WriteLine("Usage:");
                Console.WriteLine("  create_words <transactional-id>");
                Console.WriteLine("  reverse_words <transactional-id>");
                Console.WriteLine("  print_words");
                completedCancellationTokenSource.Cancel();
                return;
            }

            var command = args[0];
            string transactionalId = null;
            if (args.Length > 1) transactionalId = args[1];

            IConfiguration configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("./appsettings.json")
                .Build();

            var adminClientConfig = configuration.GetSection("AdminClient").Get<AdminClientConfig>();
            using var adminClient = new AdminClientBuilder(adminClientConfig).Build();

            switch (command)
            {
                case "create_words":
                    await WordCreator.Execute(configuration, transactionalId, adminClient, cts);
                    break;
                case "reverse_words":
                    await WordReverser.Execute(configuration, transactionalId, adminClient, cts);
                    break;
                case "print_words":
                    await WordPrinter.Execute(configuration, adminClient, cts);
                    break;
                default:
                    logger.LogError("Unknown command");
                    break;
            }

            completedCancellationTokenSource.Cancel();
        }
    }

    public class WordCreatorConfig
    {
        public TopicSpecification OutputTopic { get; set; }

        public TimeSpan ProduceRate { get; set; }

        public TimeSpan RetryInterval { get; set; }

        public TimeSpan LocalTransactionOperationTimeout { get; set; }

        public ProducerConfig Producer { get; set; }
    }

    /// <summary>
    ///     Generates random words and sends them to a topic with
    ///     exactly once semantics. This is the producer-only example.
    /// </summary>
    public static class WordCreator
    {
        private static readonly ILogger logger = Utils.LoggerFactory.CreateLogger(typeof(WordCreator));

        private static readonly Random random = new();

        private static readonly List<string> words = new();

        private static WordCreatorConfig config;

        private static string CreateWord()
        {
            int start = (int)'a';
            int end = (int)'z';
            int length = random.Next(5, 10);
            var sb = new StringBuilder(10);
            char nextChar = (char)random.Next(start, end + 1);
            for (int i = 0; i < length; i++)
            {
                sb.Append(nextChar);
                nextChar = (char)random.Next(start, end + 1);
            }
            Thread.Sleep(config.ProduceRate);
            return sb.ToString();
        }

        public static async Task Execute(IConfiguration configuration, string transactionalId, IAdminClient adminClient, CancellationTokenSource cts)
        {
            config = configuration.GetSection("WordCreator").Get<WordCreatorConfig>();
            config.Producer.ThrowIfContainsNonUserConfigurable();
            config.Producer.TransactionalId = transactionalId;
            await Utils.CreateTopicMaybe(adminClient, config.OutputTopic.Name, config.OutputTopic.NumPartitions);

            using var processor =
                    new Processor<Null, string,
                                               Null, string>
                    {
                        RetryInterval = config.RetryInterval,
                        LocalTransactionOperationTimeout = config.LocalTransactionOperationTimeout,
                        Process = Process,
                        EndTransaction = EndTransaction,
                        ProducerConfig = config.Producer,
                        CancellationTokenSource = cts
                    };
            await Task.Run(() => { processor.RunLoop(); });
            logger.LogInformation("Process finished");
        }

        public static List<ProduceMessage<Null, string>> Process(ConsumeResult<Null, string> _)
        {
            if (!words.Any())
            {
                words.Add(CreateWord());
            }
            var ret = new List<ProduceMessage<Null, string>>();
            foreach (var word in words)
            {
                ret.Add(new ProduceMessage<Null, string>()
                {
                    Topic = config.OutputTopic.Name,
                    Message = new Message<Null, string> { Value = word }
                });
            }
            return ret;
        }

        public static void EndTransaction(ICollection<ConsumeResult<Null, string>> _,
                                   ICollection<ProduceMessage<Null, string>> _1,
                                   bool committed)
        {
            if (committed)
            {
                logger.LogInformation("Produced one word");
                words.Clear();
            }
            else
            {
                // Retry the same word
            }
        }
    }


    public class WordReverserConfig
    {
        public TopicSpecification InputTopic { get; set; }

        public TopicSpecification OutputTopic { get; set; }

        public int CommitMaxMessages { get; set; }

        public TimeSpan CommitPeriod { get; set; }

        public TimeSpan CommitTimeout { get; set; }

        public TimeSpan RetryInterval { get; set; }

        public TimeSpan LocalTransactionOperationTimeout { get; set; }

        public ProducerConfig Producer { get; set; }

        public ConsumerConfig Consumer { get; set; }
    }

    /// <summary>
    ///     Read words from a topic, reverse them, and send them to a different topic with
    ///     exactly once semantics. This is the producer and consumer example.
    /// </summary>
    public static class WordReverser
    {
        private static readonly ILogger logger = Utils.LoggerFactory.CreateLogger(typeof(WordReverser));

        private static WordReverserConfig config;

        private static string Reverse(string original)
        {
            char[] originalChars = original.ToCharArray();
            Array.Reverse(originalChars);
            return new string(originalChars);
        }

        public static async Task Execute(IConfiguration configuration, string transactionalId, IAdminClient adminClient, CancellationTokenSource cts)
        {
            config = configuration.GetSection("WordReverser").Get<WordReverserConfig>();
            config.Consumer.ThrowIfContainsNonUserConfigurable();
            config.Producer.ThrowIfContainsNonUserConfigurable();
            config.Producer.TransactionalId = transactionalId;

            await Utils.CreateTopicMaybe(adminClient, config.InputTopic.Name, config.InputTopic.NumPartitions);
            await Utils.CreateTopicMaybe(adminClient, config.OutputTopic.Name, config.OutputTopic.NumPartitions);

            using var transactionalProcessor =
                    new Processor<Null, string,
                                               Null, string>
                    {
                        RetryInterval = config.RetryInterval,
                        ProducerConfig = config.Producer,
                        ConsumerConfig = config.Consumer,
                        CommitMaxMessages = config.CommitMaxMessages,
                        CommitPeriod = config.CommitPeriod,
                        LocalTransactionOperationTimeout = config.LocalTransactionOperationTimeout,
                        InputTopics = new List<string>()
                        {
                            config.InputTopic.Name
                        },
                        Process = Process,
                        EndTransaction = EndTransaction,
                        CancellationTokenSource = cts
                    };
            try
            {
                await Task.Run(() =>
                {
                    transactionalProcessor.RunLoop();
                });
            }
            catch (Exception e)
            {
                logger.LogError("Consumer Exception type: {Type}", e.GetType());
                if (e is OperationCanceledException)
                {
                    cts.Cancel();
                }
                else
                {
                    logger.LogError("Caught inner exception: {Message}", e.Message);
                }
            }
            logger.LogInformation("Process finished");
        }

        public static List<ProduceMessage<Null, string>> Process(ConsumeResult<Null, string> consumeResult)
        {
            var ret = new List<ProduceMessage<Null, string>>();
            if (consumeResult != null && consumeResult.Message != null)
            {
                ret.Add(new ProduceMessage<Null, string>()
                {
                    Topic = config.OutputTopic.Name,
                    Message = new Message<Null, string> { Value = Reverse(consumeResult.Message.Value) }
                });
            }
            return ret;
        }

        public static void EndTransaction(ICollection<ConsumeResult<Null, string>> _,
                                          ICollection<ProduceMessage<Null, string>> _1,
                                          bool committed)
        {
            if (committed)
            {
                //  If there's an application transaction involved too, 
                //  this method should commit that
                //  transaction too, otherwise any program failure can lead
                //  to a half-committed state.
            }
            else
            {
                //  Retry the same messages after rewind.
                //  If there's an application transaction involved too, 
                //  this method should abort that
                //  transaction too, otherwise any program failure can lead
                //  to a half-committed state.
            }
        }
    }

    public class WordPrinterConfig
    {
        public TopicSpecification InputTopic { get; set; }

        public int CommitMaxMessages { get; set; }

        public TimeSpan CommitPeriod { get; set; }

        public TimeSpan CommitTimeout { get; set; }

        public TimeSpan RetryInterval { get; set; }

        public ConsumerConfig Consumer { get; set; }
    }

    /// <summary>
    ///     Reads words from a topic, prints them to standard output with
    ///     exactly once semantics. This is the consumer-only example.
    /// </summary>
    public static class WordPrinter
    {
        private static readonly ILogger logger = Utils.LoggerFactory.CreateLogger(typeof(WordPrinter));

        private static WordPrinterConfig config;

        private static List<string> words = new();

        private static void Print(List<string> words)
        {
            foreach (var word in words)
            {
                Console.WriteLine(word);
            }
        }

        public static async Task Execute(IConfiguration configuration, IAdminClient adminClient, CancellationTokenSource cts)
        {
            config = configuration.GetSection("WordPrinter").Get<WordPrinterConfig>();
            config.Consumer.ThrowIfContainsNonUserConfigurable();

            await Utils.CreateTopicMaybe(adminClient, config.InputTopic.Name, config.InputTopic.NumPartitions);

            using var transactionalProcessor =
                    new Processor<Null, string,
                                               Null, string>
                    {
                        RetryInterval = config.RetryInterval,
                        ConsumerConfig = config.Consumer,
                        CommitMaxMessages = config.CommitMaxMessages,
                        CommitPeriod = config.CommitPeriod,
                        InputTopics = new List<string>()
                        {
                            config.InputTopic.Name
                        },
                        Process = Process,
                        EndTransaction = EndTransaction,
                        CancellationTokenSource = cts
                    };
            try
            {
                await Task.Run(() =>
                {
                    transactionalProcessor.RunLoop();
                });
            }
            catch (Exception e)
            {
                logger.LogError("Consumer Exception type: {Type}", e.GetType());
                if (e is OperationCanceledException)
                {
                    cts.Cancel();
                }
                else
                {
                    logger.LogError("Caught inner exception: {Message}", e.Message);
                }
            }
            logger.LogInformation("Process finished");
        }

        public static List<ProduceMessage<Null, string>> Process(ConsumeResult<Null, string> consumeResult)
        {
            if (consumeResult != null && consumeResult.Message != null)
            {
                words.Add(consumeResult.Message.Value);
            }
            return new List<ProduceMessage<Null, string>>();
        }

        public static void EndTransaction(ICollection<ConsumeResult<Null, string>> _,
                                   ICollection<ProduceMessage<Null, string>> _1,
                                   bool committed)
        {
            if (committed)
            {
                Print(words);
                //  If there's an application transaction involved too, 
                //  this method should commit that
                //  transaction too, otherwise any program failure can lead
                //  to a half-committed state.
            }
            else
            {
                //  Retry the same messages after rewind.
                //  If there's an application transaction involved too, 
                //  this method should abort that
                //  transaction too, otherwise any program failure can lead
                //  to a half-committed state.
            }
            words.Clear();
        }
    }
}
