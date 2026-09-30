// Copyright 2016-2017 Confluent Inc., 2015-2016 Andreas Heider
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

// All public types that were moved to Confluent.Kafka.Abstractions are forwarded
// here so that existing code compiled against Confluent.Kafka continues to work
// without recompilation (binary-compatible type forwarding).

using System.Runtime.CompilerServices;

[assembly: TypeForwardedTo(typeof(Confluent.Kafka.ConsumeResult<,>))]
[assembly: TypeForwardedTo(typeof(Confluent.Kafka.IConsumerGroupMetadata))]
[assembly: TypeForwardedTo(typeof(Confluent.Kafka.DeliveryReport<,>))]
[assembly: TypeForwardedTo(typeof(Confluent.Kafka.DeliveryResult<,>))]
[assembly: TypeForwardedTo(typeof(Confluent.Kafka.Error))]
[assembly: TypeForwardedTo(typeof(Confluent.Kafka.ErrorCode))]
[assembly: TypeForwardedTo(typeof(Confluent.Kafka.ErrorCodeExtensions))]
[assembly: TypeForwardedTo(typeof(Confluent.Kafka.Handle))]
[assembly: TypeForwardedTo(typeof(Confluent.Kafka.Header))]
[assembly: TypeForwardedTo(typeof(Confluent.Kafka.Headers))]
[assembly: TypeForwardedTo(typeof(Confluent.Kafka.IClient))]
[assembly: TypeForwardedTo(typeof(Confluent.Kafka.IConsumer<,>))]
[assembly: TypeForwardedTo(typeof(Confluent.Kafka.IHeader))]
[assembly: TypeForwardedTo(typeof(Confluent.Kafka.IProducer<,>))]
[assembly: TypeForwardedTo(typeof(Confluent.Kafka.KafkaException))]
[assembly: TypeForwardedTo(typeof(Confluent.Kafka.Message<,>))]
[assembly: TypeForwardedTo(typeof(Confluent.Kafka.MessageMetadata))]
[assembly: TypeForwardedTo(typeof(Confluent.Kafka.MessageNullException))]
[assembly: TypeForwardedTo(typeof(Confluent.Kafka.Offset))]
[assembly: TypeForwardedTo(typeof(Confluent.Kafka.Partition))]
[assembly: TypeForwardedTo(typeof(Confluent.Kafka.PersistenceStatus))]
[assembly: TypeForwardedTo(typeof(Confluent.Kafka.Timestamp))]
[assembly: TypeForwardedTo(typeof(Confluent.Kafka.TimestampType))]
[assembly: TypeForwardedTo(typeof(Confluent.Kafka.TopicPartition))]
[assembly: TypeForwardedTo(typeof(Confluent.Kafka.TopicPartitionOffset))]
[assembly: TypeForwardedTo(typeof(Confluent.Kafka.TopicPartitionOffsetError))]
[assembly: TypeForwardedTo(typeof(Confluent.Kafka.TopicPartitionTimestamp))]
[assembly: TypeForwardedTo(typeof(Confluent.Kafka.WatermarkOffsets))]
[assembly: TypeForwardedTo(typeof(Confluent.Kafka.Config))]
[assembly: TypeForwardedTo(typeof(Confluent.Kafka.IAsyncSerializer<>))]
[assembly: TypeForwardedTo(typeof(Confluent.Kafka.IAsyncDeserializer<>))]
[assembly: TypeForwardedTo(typeof(Confluent.Kafka.MessageComponentType))]
[assembly: TypeForwardedTo(typeof(Confluent.Kafka.SerializationContext))]
