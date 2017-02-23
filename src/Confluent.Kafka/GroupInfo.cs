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
// Derived from: rdkafka-dotnet, licensed under the 2-clause BSD License.
//
// Refer to LICENSE for more information.

using System.Collections.Generic;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Encapsulates information describing a particular
    ///     Kafka group.
    /// </summary>
    public class GroupInfo
    {
        public GroupInfo(BrokerMetadata broker, string grp, Error error, string state, string protocolType, string protocol, List<GroupMemberInfo> members)
        {
            Broker = broker;
            Group = grp;
            Error = error;
            State = state;
            ProtocolType = protocolType;
            Protocol = protocol;
            Members = members;
        }

        /// <summary>
        ///     Originating-broker info.
        /// </summary>
        public BrokerMetadata Broker { get; }

        /// <summary>
        ///     Group name
        /// </summary>
        public string Group { get; }

        /// <summary>
        ///     Broker-originated error
        /// </summary>
        public Error Error { get; }

        /// <summary>
        ///     Group state
        /// </summary>
        public string State { get; }

        /// <summary>
        ///     Group protocol type
        /// </summary>
        public string ProtocolType { get; }

        /// <summary>
        ///     Group protocol
        /// </summary>
        public string Protocol { get; }

        /// <summary>
        ///     Group members
        /// </summary>
        public List<GroupMemberInfo> Members { get; }
    }
}
