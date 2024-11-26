// Copyright 2016-2018 Confluent Inc.
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

namespace Confluent.Kafka
{
    /// <summary>
    /// Models the result of a call to ProduceAsync.
    /// </summary>
	public readonly record struct ProduceResult
    {
        /// <summary>
        /// The partition message was produced to.
        /// </summary>
        public Partition Partition { get; }

		/// <summary>
		/// The offset message was persisted at.
		/// </summary>
		public Offset Offset { get; }

		/// <summary>
		/// The persistence status of the message.
		/// </summary>
		public PersistenceStatus PersistenceStatus { get; }

        /// <summary>
        /// Creates new result.
        /// </summary>
        /// <param name="partition"></param>
        /// <param name="offset"></param>
        /// <param name="persistenceStatus"></param>
        public ProduceResult(Partition partition, Offset offset, PersistenceStatus persistenceStatus)
        {
            Partition = partition;
            Offset = offset;
            PersistenceStatus = persistenceStatus;
		}
    }
}
