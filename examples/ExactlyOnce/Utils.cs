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
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;


namespace Confluent.Kafka.Examples.ExactlyOnce
{
    public class Utils
    {
        public static readonly ILoggerFactory LoggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(builder =>
            builder.AddSimpleConsole(options =>
            {
                options.IncludeScopes = true;
                options.SingleLine = true;
                options.TimestampFormat = "yyyy-MM-ddTHH:mm:ss.fffZ ";
                options.UseUtcTimestamp = true;
            }));

        private static readonly ILogger logger = Utils.LoggerFactory.CreateLogger("Utils");

        public static async Task CreateTopicMaybe(IAdminClient adminClient, string name, int partitions)
        {
            try
            {
                await adminClient.CreateTopicsAsync(new List<TopicSpecification>
                {
                    new TopicSpecification
                    {
                        Name = name,
                        NumPartitions = partitions
                    }
                });
                logger.LogInformation("Created topic {Name}", name);
            }
            catch (CreateTopicsException e)
            {
                if (e.Error.Code != ErrorCode.Local_Partial || e.Results.Any(r => r.Error.Code != ErrorCode.TopicAlreadyExists))
                {
                    throw e;
                }
                logger.LogInformation("Topic '{Name}' already exists.", name);
            }
            catch (Exception e)
            {
                logger.LogError("Error occurred creating topic '{Name}': {Message}.", name, e.Message);
                throw;
            }
        }
    }
}
