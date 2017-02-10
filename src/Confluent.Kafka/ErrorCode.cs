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


namespace Confluent.Kafka
{
    /// <summary>
    ///     Enumeration of client and server generated error codes.
    /// </summary>
    public enum ErrorCode
    {
        /// <summary>
        ///     Received message is incorrect
        /// </summary>
        Local_BadMsg = -199,

        /// <summary>
        ///     Bad/unknown compression
        /// </summary>
        Local_BadCompression = -198,

        /// <summary>
        ///     Broker is going away
        /// </summary>
        Local_Destroy = -197,

        /// <summary>
        ///     Generic failure
        /// </summary>
        Local_Fail = -196,

        /// <summary>
        ///     Broker transport failure
        /// </summary>
        Local_Transport = -195,

        /// <summary>
        ///     Critical system resource
        /// </summary>
        Local_CritSysResource = -194,

        /// <summary>
        ///     Failed to resolve broker
        /// </summary>
        Local_Resolve = -193,

        /// <summary>
        ///     Produced message timed out
        /// </summary>
        Local_MsgTimedOut = -192,

        /// <summary>
        ///     Reached the end of the topic+partition queue on the broker. Not really an error.
        /// </summary>
        Local_PartitionEof = -191,

        /// <summary>
        ///     Permanent: Partition does not exist in cluster.
        /// </summary>
        Local_UnknownPartition = -190,

        /// <summary>
        ///     File or filesystem error
        /// </summary>
        Local_FS = -189,

        /// <summary>
        ///     Permanent: Topic does not exist in cluster.
        /// </summary>
        Local_UnknownTopic = -188,

        /// <summary>
        ///     All broker connections are down.
        /// </summary>
        Local_AllBrokersDown = -187,

        /// <summary>
        ///     Invalid argument, or invalid configuration
        /// </summary>
        Local_InvalidArg = -186,

        /// <summary>
        ///     Operation timed out
        /// </summary>
        Local_TimedOut = -185,

        /// <summary>
        ///     Queue is full
        /// </summary>
        Local_QueueFull = -184,

        /// <summary>
        ///     ISR count &lt; required.acks
        /// </summary>
        Local_IsrInsuff = -183,

        /// <summary>
        ///     Broker node update
        /// </summary>
        Local_NodeUpdate = -182,

        /// <summary>
        ///     SSL error
        /// </summary>
        Local_Ssl = -181,

        /// <summary>
        ///     Waiting for coordinator to become available.
        /// </summary>
        Local_WaitCoord = -180,

        /// <summary>
        ///     Unknown client group
        /// </summary>
        Local_UnknownGroup = -179,

        /// <summary>
        ///     Operation in progress
        /// </summary>
        Local_InProgress = -178,

        /// <summary>
        ///     Previous operation in progress, wait for it to finish.
        /// </summary>
        Local_PrevInProgress = -177,

        /// <summary>
        ///     This operation would interfere with an existing subscription
        /// </summary>
        Local_ExistingSubscription = -176,

        /// <summary>
        ///     Assigned partitions (rebalance_cb)
        /// </summary>
        Local_AssignPartitions=  -175,

        /// <summary>
        ///     Revoked partitions (rebalance_cb)
        /// </summary>
        Local_RevokePartitions = -174,

        /// <summary>
        ///     Conflicting use
        /// </summary>
        Local_Conflict = -173,

        /// <summary>
        ///     Wrong state
        /// </summary>
        Local_State = -172,

        /// <summary>
        ///     Unknown protocol
        /// </summary>
        Local_UnknownProtocol = -171,

        /// <summary>
        ///     Not implemented
        /// </summary>
        Local_NotImplemented = -170,

        /// <summary>
        ///     Authentication failure
        /// </summary>
        Local_Authentication = -169,

        /// <summary>
        ///     No stored offset
        /// </summary>
        Local_NoOffset = -168,

        /// <summary>
        ///     Outdated
        /// </summary>
        Local_Outdated = -167,

        /// <summary>
        ///     Timed out in queue
        /// </summary>
        Local_TimedOutQueue = -166,


        /// <summary>
        ///     Unknown broker error
        /// </summary>
        Unknown = -1,

        /// <summary>
        ///     Success
        /// </summary>
        NoError = 0,

        /// <summary>
        ///     Offset out of range
        /// </summary>
        Broker_OffsetOutOfRange = 1,

        /// <summary>
        ///     Invalid message
        /// </summary>
        Broker_InvalidMsg = 2,

        /// <summary>
        ///     Unknown topic or partition
        /// </summary>
        Broker_UnknownTopicOrPart = 3,

        /// <summary>
        ///     Invalid message size
        /// </summary>
        Broker_InvalidMsgSize = 4,

        /// <summary>
        ///     Leader not available
        /// </summary>
        Broker_LeaderNotAvailable = 5,

        /// <summary>
        ///     Not leader for partition
        /// </summary>
        Broker_NotLeaderForPartition = 6,

        /// <summary>
        ///     Request timed out
        /// </summary>
        Broker_RequestTimedOut = 7,

        /// <summary>
        ///     Broker not available
        /// </summary>
        Broker_BrokerNotAvailable = 8,

        /// <summary>
        ///     Replica not available
        /// </summary>
        Broker_ReplicaNotAvailable = 9,

        /// <summary>
        ///     Message size too large
        /// </summary>
        Broker_MsgSizeTooLarge = 10,

        /// <summary>
        ///     StaleControllerEpochCode
        /// </summary>
        Broker_StaleCtrlEpoch = 11,

        /// <summary>
        ///     Offset metadata string too large
        /// </summary>
        Broker_OffsetMetadataTooLarge = 12,

        /// <summary>
        ///     Broker disconnected before response received
        /// </summary>
        Broker_NetworkException = 13,

        /// <summary>
        ///     Group coordinator load in progress
        /// </summary>
        Broker_GroupLoadInProress = 14,

        /// <summary>
        /// Group coordinator not available
        /// </summary>
        Broker_GroupCoordinatorNotAvailable = 15,

        /// <summary>
        ///     Not coordinator for group
        /// </summary>
        Broker_NotCoordinatorForGroup = 16,

        /// <summary>
        ///     Invalid topic
        /// </summary>
        Broker_TopicException = 17,

        /// <summary>
        ///     Message batch larger than configured server segment size
        /// </summary>
        Broker_RecordListTooLarge = 18,

        /// <summary>
        ///     Not enough in-sync replicas
        /// </summary>
        Broker_NotEnoughReplicas = 19,

        /// <summary>
        ///     Message(s) written to insufficient number of in-sync replicas
        /// </summary>
        Broker_NotEnoughReplicasAfterAppend = 20,

        /// <summary>
        ///     Invalid required acks value
        /// </summary>
        Broker_InvalidRequiredAcks = 21,

        /// <summary>
        ///     Specified group generation id is not valid
        /// </summary>
        Broker_IllegalGeneration = 22,

        /// <summary>
        ///     Inconsistent group protocol
        /// </summary>
        Broker_InconsistentGroupProtocol = 23,

        /// <summary>
        ///     Invalid group.id
        /// </summary>
        Broker_InvalidGroupId = 24,

        /// <summary>
        ///     Unknown member
        /// </summary>
        Broker_UnknownMemberId = 25,

        /// <summary>
        ///     Invalid session timeout
        /// </summary>
        Broker_InvalidSessionTimeout = 26,

        /// <summary>
        ///     Group rebalance in progress
        /// </summary>
        Broker_RebalanceInProgress = 27,

        /// <summary>
        ///     Commit offset data size is not valid
        /// </summary>
        Broker_InvalidCommitOffsetSize = 28,

        /// <summary>
        ///     Topic authorization failed
        /// </summary>
        Broker_TopicAuthorizationFailed = 29,

        /// <summary>
        ///     Group authorization failed
        /// </summary>
        Broker_GroupAuthorizationFailed = 30,

        /// <summary>
        ///     Cluster authorization failed
        /// </summary>
        Broker_ClusterAuthorizationFailed = 31,

        /// <summary>
        ///     Invalid timestamp
        /// </summary>
        Broker_InvalidTimestamp = 32,

        /// <summary>
        ///     Unsupported SASL mechanism
        /// </summary>
        Broker_UnsupportedSaslMechanism = 33,

        /// <summary>
        ///     Illegal SASL state
        /// </summary>
        Broker_IllegalSaslState = 34,

        /// <summary>
        ///     Unuspported version
        /// </summary>
        Broker_UnsupportedVersion = 35
    };

    public static class ErrorCodeExtensions
    {
        public static string GetReason(this ErrorCode code)
        {
            return Internal.Util.Marshal.PtrToStringUTF8(Impl.LibRdKafka.err2str(code));
        }
    }
}
