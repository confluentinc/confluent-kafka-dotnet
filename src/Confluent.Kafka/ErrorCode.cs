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
    ///     Internal errors to rdkafka are prefixed with _
    /// </summary>
    public enum ErrorCode
    {
        /// <summary>Begin internal error codes</summary>
        RD_BEGIN = -200,
        /// <summary>Received message is incorrect</summary>
        RD_BAD_MSG = -199,
        /// <summary>Bad/unknown compression</summary>
        RD_BAD_COMPRESSION = -198,
        /// <summary>Broker is going away</summary>
        RD_DESTROY = -197,
        /// <summary>Generic failure</summary>
        RD_FAIL = -196,
        /// <summary>Broker transport failure</summary>
        RD_TRANSPORT = -195,
        /// <summary>Critical system resource</summary>
        RD_CRIT_SYS_RESOURCE = -194,
        /// <summary>Failed to resolve broker</summary>
        RD_RESOLVE = -193,
        /// <summary>Produced message timed out</summary>
        RD_MSG_TIMED_OUT = -192,
        /// <summary>Reached the end of the topic+partition queue on the broker. Not really an error.</summary>
        RD_PARTITION_EOF = -191,
        /// <summary>Permanent: Partition does not exist in cluster.</summary>
        RD_UNKNOWN_PARTITION = -190,
        /// <summary>File or filesystem error</summary>
        RD_FS = -189,
         /// <summary>Permanent: Topic does not exist in cluster.</summary>
        RD_UNKNOWN_TOPIC = -188,
        /// <summary>All broker connections are down.</summary>
        RD_ALL_BROKERS_DOWN = -187,
        /// <summary>Invalid argument, or invalid configuration</summary>
        RD_INVALID_ARG = -186,
        /// <summary>Operation timed out</summary>
        RD_TIMED_OUT = -185,
        /// <summary>Queue is full</summary>
        RD_QUEUE_FULL = -184,
        /// <summary>ISR count &lt; required.acks</summary>
        RD_ISR_INSUFF = -183,
        /// <summary>Broker node update</summary>
        RD_NODE_UPDATE = -182,
        /// <summary>SSL error</summary>
        RD_SSL = -181,
        /// <summary>Waiting for coordinator to become available.</summary>
        RD_WAIT_COORD = -180,
        /// <summary>Unknown client group</summary>
        RD_UNKNOWN_GROUP = -179,
        /// <summary>Operation in progress</summary>
        RD_IN_PROGRESS = -178,
        /// <summary>Previous operation in progress, wait for it to finish.</summary>
        RD_PREV_IN_PROGRESS = -177,
        /// <summary>This operation would interfere with an existing subscription</summary>
        RD_EXISTING_SUBSCRIPTION = -176,
        /// <summary>Assigned partitions (rebalance_cb)</summary>
        RD_ASSIGN_PARTITIONS = -175,
        /// <summary>Revoked partitions (rebalance_cb)</summary>
        RD_REVOKE_PARTITIONS = -174,
        /// <summary>Conflicting use</summary>
        RD_CONFLICT = -173,
        /// <summary>Wrong state</summary>
        RD_STATE = -172,
        /// <summary>Unknown protocol</summary>
        RD_UNKNOWN_PROTOCOL = -171,
        /// <summary>Not implemented</summary>
        RD_NOT_IMPLEMENTED = -170,
        /// <summary>Authentication failure</summary>
        RD_AUTHENTICATION = -169,
        /// <summary>No stored offset</summary>
        RD_NO_OFFSET = -168,
        ///<summary>Outdated</summary>
        RD_OUTDATED = -167,
        /// <summary>Timed out in queue</summary>
        RD_TIMED_OUT_QUEUE = -166,
        /// <summary>End internal error codes</summary>
        RD_END = -100,

        // Kafka broker errors:
        /// <summary>Unknown broker error</summary>
        UNKNOWN = -1,
        /// <summary>Success</summary>
        NO_ERROR = 0,
        /// <summary>Offset out of range</summary>
        OFFSET_OUT_OF_RANGE = 1,
        /// <summary>Invalid message</summary>
        INVALID_MSG = 2,
        /// <summary>Unknown topic or partition</summary>
        UNKNOWN_TOPIC_OR_PART = 3,
        /// <summary>Invalid message size</summary>
        INVALID_MSG_SIZE = 4,
        /// <summary>Leader not available</summary>
        LEADER_NOT_AVAILABLE = 5,
        /// <summary>Not leader for partition</summary>
        NOT_LEADER_FOR_PARTITION = 6,
        /// <summary>Request timed out</summary>
        REQUEST_TIMED_OUT = 7,
        /// <summary>Broker not available</summary>
        BROKER_NOT_AVAILABLE = 8,
        /// <summary>Replica not available</summary>
        REPLICA_NOT_AVAILABLE = 9,
        /// <summary>Message size too large</summary>
        MSG_SIZE_TOO_LARGE = 10,
        /// <summary>StaleControllerEpochCode</summary>
        STALE_CTRL_EPOCH = 11,
        /// <summary>Offset metadata string too large</summary>
        OFFSET_METADATA_TOO_LARGE = 12,
        /// <summary>Broker disconnected before response received</summary>
        NETWORK_EXCEPTION = 13,
        /// <summary>Group coordinator load in progress</summary>
        GROUP_LOAD_IN_PROGRESS = 14,
         /// <summary>Group coordinator not available</summary>
        GROUP_COORDINATOR_NOT_AVAILABLE = 15,
        /// <summary>Not coordinator for group</summary>
        NOT_COORDINATOR_FOR_GROUP = 16,
        /// <summary>Invalid topic</summary>
        TOPIC_EXCEPTION = 17,
        /// <summary>Message batch larger than configured server segment size</summary>
        RECORD_LIST_TOO_LARGE = 18,
        /// <summary>Not enough in-sync replicas</summary>
        NOT_ENOUGH_REPLICAS = 19,
        /// <summary>Message(s) written to insufficient number of in-sync replicas</summary>
        NOT_ENOUGH_REPLICAS_AFTER_APPEND = 20,
        /// <summary>Invalid required acks value</summary>
        INVALID_REQUIRED_ACKS = 21,
        /// <summary>Specified group generation id is not valid</summary>
        ILLEGAL_GENERATION = 22,
        /// <summary>Inconsistent group protocol</summary>
        INCONSISTENT_GROUP_PROTOCOL = 23,
        /// <summary>Invalid group.id</summary>
        INVALID_GROUP_ID = 24,
        /// <summary>Unknown member</summary>
        UNKNOWN_MEMBER_ID = 25,
        /// <summary>Invalid session timeout</summary>
        INVALID_SESSION_TIMEOUT = 26,
        /// <summary>Group rebalance in progress</summary>
        REBALANCE_IN_PROGRESS = 27,
        /// <summary>Commit offset data size is not valid</summary>
        INVALID_COMMIT_OFFSET_SIZE = 28,
        /// <summary>Topic authorization failed</summary>
        TOPIC_AUTHORIZATION_FAILED = 29,
        /// <summary>Group authorization failed</summary>
        GROUP_AUTHORIZATION_FAILED = 30,
        /// <summary>Cluster authorization failed</summary>
        CLUSTER_AUTHORIZATION_FAILED = 31,
        /// <summary>Invalid timestamp</summary>
        INVALID_TIMESTAMP = 32,
        /// <summary> Unsupported SASL mechanism</summary>
        UNSUPPORTED_SASL_MECHANISM = 33,
        /// <summary>Illegal SASL state</summary>
        ILLEGAL_SASL_STATE = 34,
        /// <summary>Unuspported version</summary>
        UNSUPPORTED_VERSION = 35,
    };

    public static class ErrorCodeExtensions
    {
        public static string GetReason(this ErrorCode code)
        {
            return Internal.Util.Marshal.PtrToStringUTF8(Impl.LibRdKafka.err2str(code));
        }
    }
}
