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
        /// <summary>
        ///     Initializes a new instance of the GroupInfo class with only the Group
        ///     and Error properties set. Used when this group is only listed.
        /// </summary>
        /// <param name="group">
        ///     The group name.
        /// </param>
        /// <param name="error">
        ///     A rich <see cref="Error"/> value associated with the information encapsulated by this class.
        /// </param>
        public GroupInfo(string group, Error error)
        {
            Group = group;
            Error = error;
        }

        /// <summary>
        ///     Initializes a new instance of the GroupInfo class with only the Group, State,
        ///     IsSimpleConsumerGroup and Error properties set. Used when this group is only listed.
        /// </summary>
        /// <param name="group">
        ///     The group name.
        /// </param>
        /// <param name="error">
        ///     A rich <see cref="Error"/> value associated with the information encapsulated by this class.
        /// </param>
        /// <param name="state">
        ///     The group state code.
        /// </param>
        /// <param name="isSimpleConsumerGroup">
        ///    If the group is a simple consumer group or not.
        /// </param>
        public GroupInfo(string group, Error error, ConsumerGroupState state, bool isSimpleConsumerGroup)
        {
            Group = group;
            Error = error;
            StateCode = state;
            IsSimpleConsumerGroup = isSimpleConsumerGroup;
        }

        /// <summary>
        ///     Initializes a new instance of the GroupInfo class.
        /// </summary>
        /// <param name="broker">
        ///     Originating broker info.
        /// </param>
        /// <param name="group">
        ///     The group name.
        /// </param>
        /// <param name="error">
        ///     A rich <see cref="Error"/> value associated with the information encapsulated by this class.
        /// </param>
        /// <param name="state">
        ///     The group state (string).
        /// </param>
        /// <param name="stateCode">
        ///     The group state (enum).
        /// </param>
        /// <param name="isSimpleConsumerGroup">
        ///     If the group is a simple consumer group or not.
        /// </param>
        /// <param name="protocolType">
        ///     The group protocol type.
        /// </param>
        /// <param name="protocol">
        ///     The group protocol.
        /// </param>
        /// <param name="members">
        ///     The group members.
        /// </param>
        public GroupInfo(BrokerMetadata broker, string group, Error error, string state, 
                         ConsumerGroupState stateCode, bool isSimpleConsumerGroup, string protocolType, 
                         string protocol, List<GroupMemberInfo> members)
        {
            Broker = broker;
            Group = group;
            Error = error;
            State = state;
            StateCode = stateCode;
            IsSimpleConsumerGroup = isSimpleConsumerGroup;
            ProtocolType = protocolType;
            Protocol = protocol;
            Members = members;
        }

        /// <summary>
        ///     Gets the originating-broker info (coordinator)
        /// </summary>
        public BrokerMetadata Broker { get; }

        /// <summary>
        ///     Gets the group name
        /// </summary>
        public string Group { get; }

        /// <summary>
        ///     Gets a rich <see cref="Error"/> value associated with the information encapsulated by this class.
        /// </summary>
        public Error Error { get; }

        /// <summary>
        ///     DEPRECATED. Use StateCode instead.
        ///
        ///     Gets the group state (string representation)
        /// </summary>
        public string State { get; }

        /// <summary>
        ///     Gets the group state (enum representation)
        /// </summary>
        public ConsumerGroupState StateCode { get; }

        /// <summary>
        ///     Gets if this group is a simple consumer group
        /// </summary>
        public bool IsSimpleConsumerGroup { get; }

        /// <summary>
        ///     Gets the group protocol type
        /// </summary>
        public string ProtocolType { get; }

        /// <summary>
        ///     Gets the group protocol (partition assignor)
        /// </summary>
        public string Protocol { get; }

        /// <summary>
        ///     Gets the group members
        /// </summary>
        public List<GroupMemberInfo> Members { get; }
    }
}
