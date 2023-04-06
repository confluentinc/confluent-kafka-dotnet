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

using System;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Enumeration of local and broker generated error codes.
    /// </summary>
    /// <remarks>
    ///     Error codes that relate to locally produced errors in 
    ///     librdkafka are prefixed with Local_
    /// </remarks>
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
        Local_PartitionEOF = -191,

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
        ///     Feature not supported by broker
        /// </summary>
        Local_UnsupportedFeature = -165,

        /// <summary>
        ///     Awaiting cache update
        /// </summary>
        Local_WaitCache = -164,

        /// <summary>
        ///     Operation interrupted
        /// </summary>
        Local_Intr = -163,

        /// <summary>
        ///     Key serialization error
        /// </summary>
        Local_KeySerialization = -162,

        /// <summary>
        ///     Value serialization error
        /// </summary>
        Local_ValueSerialization = -161,

        /// <summary>
        ///     Key deserialization error
        /// </summary>
        Local_KeyDeserialization = -160,

        /// <summary>
        ///     Value deserialization error
        /// </summary>
        Local_ValueDeserialization = -159,

        /// <summary>
        ///     Partial response
        /// </summary>
        Local_Partial = -158,

        /// <summary>
        ///     Modification attempted on read-only object
        /// </summary>
        Local_ReadOnly = -157,

        /// <summary>
        ///     No such entry / item not found
        /// </summary>
        Local_NoEnt = -156,

        /// <summary>
        ///     Read underflow
        /// </summary>
        Local_Underflow = -155,

        /// <summary>
        ///     Invalid type
        /// </summary>
        Local_InvalidType = -154,

        /// <summary>
        ///     Retry operation.
        /// </summary>
        Local_Retry = -153,

        /// <summary>
        ///     Purged in queue
        /// </summary>
        Local_PurgeQueue = -152,

        /// <summary>
        ///     Purged in flight
        /// </summary>
        Local_PurgeInflight = -151,

        /// <summary>
        ///     Fatal error: see rd_kafka_fatal_error() 
        /// </summary>
        Local_Fatal = -150,

        /// <summary>
        ///     Inconsistent state
        /// </summary>
        Local_Inconsistent = -149,

        /// <summary>
        ///     Gap-less ordering would not be guaranteed if proceeding
        /// </summary>
        Local_GaplessGuarantee = -148,

        /// <summary>
        ///     Maximum poll interval exceeded 
        /// </summary>
        Local_MaxPollExceeded = -147,

        /// <summary>
        ///     Unknown broker
        /// </summary>
        Local_UnknownBroker = -146,

        /// <summary>
        ///     Functionality not configured
        /// </summary>
        Local_NotConfigured = -145,

        /// <summary>
        ///     Instance has been fenced
        /// </summary>
        Local_Fenced = -144,

        /// <summary>
        ///     Application generated exception.
        /// </summary>
        Local_Application = -143,

        /// <summary>
        ///     Assignment lost
        /// </summary>
        Local_AssignmentLost = -142,

        /// <summary>
        ///     No operation performed
        /// </summary>
        Local_Noop = -141,

        /// <summary>
        ///     No offset to automatically reset to
        /// </summary>
        Local_AutoOffsetReset = -140,
        
        /// <summary>
        ///     Partition log truncation detected
        /// </summary> 
        Local_LogTruncation = -139,

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
        OffsetOutOfRange = 1,

        /// <summary>
        ///     Invalid message
        /// </summary>
        InvalidMsg = 2,

        /// <summary>
        ///     Unknown topic or partition
        /// </summary>
        UnknownTopicOrPart = 3,

        /// <summary>
        ///     Invalid message size
        /// </summary>
        InvalidMsgSize = 4,

        /// <summary>
        ///     Leader not available
        /// </summary>
        LeaderNotAvailable = 5,

        /// <summary>
        ///     Not leader for partition
        /// </summary>
        NotLeaderForPartition = 6,

        /// <summary>
        ///     Request timed out
        /// </summary>
        RequestTimedOut = 7,

        /// <summary>
        ///     Broker not available
        /// </summary>
        BrokerNotAvailable = 8,

        /// <summary>
        ///     Replica not available
        /// </summary>
        ReplicaNotAvailable = 9,

        /// <summary>
        ///     Message size too large
        /// </summary>
        MsgSizeTooLarge = 10,

        /// <summary>
        ///     StaleControllerEpochCode
        /// </summary>
        StaleCtrlEpoch = 11,

        /// <summary>
        ///     Offset metadata string too large
        /// </summary>
        OffsetMetadataTooLarge = 12,

        /// <summary>
        ///     Broker disconnected before response received
        /// </summary>
        NetworkException = 13,

        /// <summary>
        ///     Group coordinator load in progress
        /// </summary>
        [Obsolete("Superseded by GroupLoadInProgress")]
        GroupLoadInProress = 14,

        /// <summary>
        ///     Group coordinator load in progress
        /// </summary>
        GroupLoadInProgress = 14,

        /// <summary>
        /// Group coordinator not available
        /// </summary>
        GroupCoordinatorNotAvailable = 15,

        /// <summary>
        ///     Not coordinator for group
        /// </summary>
        NotCoordinatorForGroup = 16,

        /// <summary>
        ///     Invalid topic
        /// </summary>
        TopicException = 17,

        /// <summary>
        ///     Message batch larger than configured server segment size
        /// </summary>
        RecordListTooLarge = 18,

        /// <summary>
        ///     Not enough in-sync replicas
        /// </summary>
        NotEnoughReplicas = 19,

        /// <summary>
        ///     Message(s) written to insufficient number of in-sync replicas
        /// </summary>
        NotEnoughReplicasAfterAppend = 20,

        /// <summary>
        ///     Invalid required acks value
        /// </summary>
        InvalidRequiredAcks = 21,

        /// <summary>
        ///     Specified group generation id is not valid
        /// </summary>
        IllegalGeneration = 22,

        /// <summary>
        ///     Inconsistent group protocol
        /// </summary>
        InconsistentGroupProtocol = 23,

        /// <summary>
        ///     Invalid group.id
        /// </summary>
        InvalidGroupId = 24,

        /// <summary>
        ///     Unknown member
        /// </summary>
        UnknownMemberId = 25,

        /// <summary>
        ///     Invalid session timeout
        /// </summary>
        InvalidSessionTimeout = 26,

        /// <summary>
        ///     Group rebalance in progress
        /// </summary>
        RebalanceInProgress = 27,

        /// <summary>
        ///     Commit offset data size is not valid
        /// </summary>
        InvalidCommitOffsetSize = 28,

        /// <summary>
        ///     Topic authorization failed
        /// </summary>
        TopicAuthorizationFailed = 29,

        /// <summary>
        ///     Group authorization failed
        /// </summary>
        GroupAuthorizationFailed = 30,

        /// <summary>
        ///     Cluster authorization failed
        /// </summary>
        ClusterAuthorizationFailed = 31,

        /// <summary>
        ///     Invalid timestamp
        /// </summary>
        InvalidTimestamp = 32,

        /// <summary>
        ///     Unsupported SASL mechanism
        /// </summary>
        UnsupportedSaslMechanism = 33,

        /// <summary>
        ///     Illegal SASL state
        /// </summary>
        IllegalSaslState = 34,

        /// <summary>
        ///     Unsupported version
        /// </summary>
        UnsupportedVersion = 35,

        /// <summary>
        ///     Topic already exists
        /// </summary>
        TopicAlreadyExists = 36,

        /// <summary>
        ///     Invalid number of partitions
        /// </summary>
        InvalidPartitions = 37,

        /// <summary>
        ///    Invalid replication factor
        /// </summary>
        InvalidReplicationFactor = 38,

        /// <summary>
        ///     Invalid replica assignment
        /// </summary>
        InvalidReplicaAssignment = 39,

        /// <summary>
        ///     Invalid config
        /// </summary>
        InvalidConfig = 40,

        /// <summary>
        ///     Not controller for cluster
        /// </summary>
        NotController = 41,

        /// <summary>
        ///     Invalid request
        /// </summary>
        InvalidRequest = 42,

        /// <summary>
        ///     Message format on broker does not support request
        /// </summary>
        UnsupportedForMessageFormat = 43,

        /// <summary>
        ///     Isolation policy violation
        /// </summary>
        PolicyViolation = 44,

        /// <summary>
        ///     Broker received an out of order sequence number
        /// </summary>
        OutOfOrderSequenceNumber = 45,

        /// <summary>
        ///     Broker received a duplicate sequence number
        /// </summary>
        DuplicateSequenceNumber = 46,

        /// <summary>
        ///     Producer attempted an operation with an old epoch
        /// </summary>
        InvalidProducerEpoch = 47,

        /// <summary>
        ///     Producer attempted a transactional operation in an invalid state
        /// </summary>
        InvalidTxnState = 48,

        /// <summary>
        ///     Producer attempted to use a producer id which is not currently assigned to its transactional id
        /// </summary>
        InvalidProducerIdMapping = 49,

        /// <summary>
        ///     Transaction timeout is larger than the maximum value allowed by the broker's max.transaction.timeout.ms
        /// </summary>
        InvalidTransactionTimeout = 50,

        /// <summary>
        ///     Producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing
        /// </summary>
        ConcurrentTransactions = 51,

        /// <summary>
        ///     Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer
        /// </summary>
        TransactionCoordinatorFenced = 52,

        /// <summary>
        ///     Transactional Id authorization failed
        /// </summary>
        TransactionalIdAuthorizationFailed = 53,

        /// <summary>
        ///     Security features are disabled
        /// </summary>
        SecurityDisabled = 54,

        /// <summary>
        ///     Operation not attempted
        /// </summary>
        OperationNotAttempted = 55,

        /// <summary>
        ///     Disk error when trying to access log file on the disk.
        /// </summary>
        KafkaStorageError = 56,

        /// <summary>
        ///     The user-specified log directory is not found in the broker config.
        /// </summary>
        LogDirNotFound = 57,

        /// <summary>
        ///     SASL Authentication failed.
        /// </summary>
        SaslAuthenticationFailed = 58,

        /// <summary>
        ///     Unknown Producer Id.
        /// </summary>
        UnknownProducerId = 59,

        /// <summary>
        ///     Partition reassignment is in progress.
        /// </summary>
        ReassignmentInProgress = 60,

        /// <summary>
        ///     Delegation Token feature is not enabled.
        /// </summary>
        DelegationTokenAuthDisabled = 61,

        /// <summary>
        ///     Delegation Token is not found on server.
        /// </summary>
        DelegationTokenNotFound = 62,

        /// <summary>
        ///     Specified Principal is not valid Owner/Renewer.
        /// </summary>
        DelegationTokenOwnerMismatch = 63, 
        
        /// <summary>
        ///     Delegation Token requests are not allowed on this connection.
        /// </summary>
        DelegationTokenRequestNotAllowed = 64,

        /// <summary>
        ///     Delegation Token authorization failed.
        /// </summary>
        DelegationTokenAuthorizationFailed = 65,

        /// <summary>
        ///     Delegation Token is expired.
        /// </summary>
        DelegationTokenExpired = 66,

        /// <summary>
        ///     Supplied principalType is not supported.
        /// </summary>
        InvalidPrincipalType = 67,

        /// <summary>
        ///     The group is not empty.
        /// </summary>
        NonEmptyGroup = 68,

        /// <summary>
        ///     The group id does not exist.
        /// </summary>
        GroupIdNotFound = 69,

        /// <summary>
        ///     The fetch session ID was not found.
        /// </summary>
        FetchSessionIdNotFound = 70,

        /// <summary>
        ///     The fetch session epoch is invalid.
        /// </summary>
        InvalidFetchSessionEpoch = 71,

        /// <summary>
        ///     No matching listener.
        /// </summary>
        ListenerNotFound = 72,

        /// <summary>
        ///     Topic deletion is disabled.
        /// </summary>
        TopicDeletionDisabled = 73,

        /// <summary>
        ///     Leader epoch is older than broker epoch.
        /// </summary>
        FencedLeaderEpoch = 74,

        /// <summary>
        ///     Leader epoch is newer than broker epoch.
        /// </summary>
        UnknownLeaderEpoch = 75,

        /// <summary>
        ///     Unsupported compression type.
        /// </summary>
        UnsupportedCompressionType = 76,

        /// <summary>
        ///     Broker epoch has changed.
        /// </summary>
        StaleBrokerEpoch = 77,

        /// <summary>
        ///     Leader high watermark is not caught up.
        /// </summary>
        OffsetNotAvailable = 78,

        /// <summary>
        ///     Group member needs a valid member ID.
        /// </summary>
        MemberIdRequired = 79,

        /// <summary>
        ///     Preferred leader was not available.
        /// </summary>
        PreferredLeaderNotAvailable = 80,

        /// <summary>
        ///     Consumer group has reached maximum size.
        /// </summary>
        GroupMaxSizeReached = 81,

        /// <summary>
        ///     Static consumer fenced by other consumer with same group.instance.id.
        /// </summary>
        FencedInstanceId = 82,

        /// <summary>
        ///     Eligible partition leaders are not available.
        /// </summary>
        EligibleLeadersNotAvailable = 83,

        /// <summary>
        ///     Leader election not needed for topic partition.
        /// </summary>
        ElectionNotNeeded = 84,

        /// <summary>
        ///     No partition reassignment is in progress.
        /// </summary>
        NoReassignmentInProgress = 85,

        /// <summary>
        ///     Deleting offsets of a topic while the consumer group is subscribed to it.
        /// </summary>
        GroupSubscribedToTopic = 86,

        /// <summary>
        /// Broker failed to validate record.
        /// </summary>
        InvalidRecord = 87,

        /// <summary>
        ///     There are unstable offsets that need to be cleared.
        /// </summary>
        UnstableOffsetCommit = 88,

        /// <summary>
        ///     Throttling quota has been exceeded.
        /// </summary>
        ThrottlingQuotaExceeded = 89,

        /// <summary>
        ///     There is a newer producer with the same transactionalId which fences the current one.
        /// </summary>
        ProducerFenced = 90,

        /// <summary>
        ///     Request illegally referred to resource that does not exist.
        /// </summary>
        ResourceNotFound = 91,

        /// <summary>
        ///     Request illegally referred to the same resource twice.
        /// </summary>
        DuplicateResource = 92,

        /// <summary>
        ///     Requested credential would not meet criteria for acceptability.
        /// </summary>
        UnacceptableCredential = 93,

        /// <summary>
        ///     Indicates that the either the sender or recipient of a voter-only request is not one of the expected voters.
        /// </summary>
        InconsistentVoterSet = 94,

        /// <summary>
        ///     Invalid update version.
        /// </summary>
        InvalidUpdateVersion = 95,

        /// <summary>
        ///     Unable to update finalized features due to server error.
        /// </summary>
        FeatureUpdateFailed = 96,

        /// <summary>
        ///     Request principal deserialization failed during forwarding.
        /// </summary>
        PrincipalDeserializationFailure = 97
    };

    /// <summary>
    ///     Provides extension methods on the ErrorCode enumeration.
    /// </summary>
    public static class ErrorCodeExtensions
    {
        /// <summary>
        ///     Returns the static error string associated with 
        ///     the particular ErrorCode value.
        /// </summary>
        public static string GetReason(this ErrorCode code)
        {
            Impl.Librdkafka.Initialize(null);
            return Internal.Util.Marshal.PtrToStringUTF8(Impl.Librdkafka.err2str(code));
        }
    }
}
