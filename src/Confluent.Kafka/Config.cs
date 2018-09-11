// *** Auto-generated *** - do not modify manually.
//
// Copyright 2018 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the 'License');
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an 'AS IS' BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;


namespace Confluent.Kafka
{
    /// <summary>
    ///     BrokerAddressFamily enum values
    /// </summary>
    public enum BrokerAddressFamilyType
    {
        /// <summary>
        ///     Any
        /// </summary>
        Any,

        /// <summary>
        ///     V4
        /// </summary>
        V4,

        /// <summary>
        ///     V6
        /// </summary>
        V6
    }

    /// <summary>
    ///     SecurityProtocol enum values
    /// </summary>
    public enum SecurityProtocolType
    {
        /// <summary>
        ///     Plaintext
        /// </summary>
        Plaintext,

        /// <summary>
        ///     Ssl
        /// </summary>
        Ssl,

        /// <summary>
        ///     Sasl_plaintext
        /// </summary>
        Sasl_plaintext,

        /// <summary>
        ///     Sasl_ssl
        /// </summary>
        Sasl_ssl
    }

    /// <summary>
    ///     QueuingStrategy enum values
    /// </summary>
    public enum QueuingStrategyType
    {
        /// <summary>
        ///     Fifo
        /// </summary>
        Fifo,

        /// <summary>
        ///     Lifo
        /// </summary>
        Lifo
    }

    /// <summary>
    ///     AutoOffsetReset enum values
    /// </summary>
    public enum AutoOffsetResetType
    {
        /// <summary>
        ///     Latest
        /// </summary>
        Latest,

        /// <summary>
        ///     Earliest
        /// </summary>
        Earliest,

        /// <summary>
        ///     Error
        /// </summary>
        Error
    }

    /// <summary>
    ///     Configuration common to all clients
    /// </summary>
    public class ClientConfig : IEnumerable<KeyValuePair<string, string>>
    {
        /// <summary>
        ///     Initialize a new empty <see cref="ClientConfig" /> instance.
        /// </summary>
        public ClientConfig() {}

        /// <summary>
        ///     Initialize a new <see cref="ClientConfig" /> instance based on
        ///     an existing <see cref="ClientConfig" /> instance.
        /// </summary>
        public ClientConfig(ClientConfig config) { this.properties = new Dictionary<string, string>(config.ToDictionary(a => a.Key, a => a.Value)); }

        /// <summary>
        ///     Initialize a new <see cref="ClientConfig" /> instance based on
        ///     an existing key/value pair collection.
        /// </summary>
        public ClientConfig(IEnumerable<KeyValuePair<string, string>> config) { this.properties = new Dictionary<string, string>(config.ToDictionary(a => a.Key, a => a.Value)); }
        /// <summary>
        ///     Client identifier.
        /// </summary>
        public string ClientId { set { this.SetObject("client.id", value); } }

        /// <summary>
        ///     Initial list of brokers as a CSV list of broker host or host:port. The application may also use `rd_kafka_brokers_add()` to add brokers during runtime.
        /// </summary>
        public string BootstrapServers { set { this.SetObject("bootstrap.servers", value); } }

        /// <summary>
        ///     Maximum Kafka protocol request message size.
        /// </summary>
        public int MessageMaxBytes { set { this.SetObject("message.max.bytes", value); } }

        /// <summary>
        ///     Maximum size for message to be copied to buffer. Messages larger than this will be passed by reference (zero-copy) at the expense of larger iovecs.
        /// </summary>
        public int MessageCopyMaxBytes { set { this.SetObject("message.copy.max.bytes", value); } }

        /// <summary>
        ///     Maximum Kafka protocol response message size. This serves as a safety precaution to avoid memory exhaustion in case of protocol hickups. This value is automatically adjusted upwards to be at least `fetch.max.bytes` + 512 to allow for protocol overhead.
        /// </summary>
        public int ReceiveMessageMaxBytes { set { this.SetObject("receive.message.max.bytes", value); } }

        /// <summary>
        ///     Maximum number of in-flight requests per broker connection. This is a generic property applied to all broker communication, however it is primarily relevant to produce requests. In particular, note that other mechanisms limit the number of outstanding consumer fetch request per broker to one.
        /// </summary>
        public int MaxInFlight { set { this.SetObject("max.in.flight", value); } }

        /// <summary>
        ///     Non-topic request timeout in milliseconds. This is for metadata requests, etc.
        /// </summary>
        public int MetadataRequestTimeoutMs { set { this.SetObject("metadata.request.timeout.ms", value); } }

        /// <summary>
        ///     Topic metadata refresh interval in milliseconds. The metadata is automatically refreshed on error and connect. Use -1 to disable the intervalled refresh.
        /// </summary>
        public int TopicMetadataRefreshIntervalMs { set { this.SetObject("topic.metadata.refresh.interval.ms", value); } }

        /// <summary>
        ///     Metadata cache max age. Defaults to topic.metadata.refresh.interval.ms * 3
        /// </summary>
        public int MetadataMaxAgeMs { set { this.SetObject("metadata.max.age.ms", value); } }

        /// <summary>
        ///     When a topic loses its leader a new metadata request will be enqueued with this initial interval, exponentially increasing until the topic metadata has been refreshed. This is used to recover quickly from transitioning leader brokers.
        /// </summary>
        public int TopicMetadataRefreshFastIntervalMs { set { this.SetObject("topic.metadata.refresh.fast.interval.ms", value); } }

        /// <summary>
        ///     Sparse metadata requests (consumes less network bandwidth)
        /// </summary>
        public bool TopicMetadataRefreshSparse { set { this.SetObject("topic.metadata.refresh.sparse", value); } }

        /// <summary>
        ///     Topic blacklist, a comma-separated list of regular expressions for matching topic names that should be ignored in broker metadata information as if the topics did not exist.
        /// </summary>
        public string TopicBlacklist { set { this.SetObject("topic.blacklist", value); } }

        /// <summary>
        ///     A comma-separated list of debug contexts to enable. Detailed Producer debugging: broker,topic,msg. Consumer: consumer,cgrp,topic,fetch
        /// </summary>
        public string Debug { set { this.SetObject("debug", value); } }

        /// <summary>
        ///     Default timeout for network requests. Producer: ProduceRequests will use the lesser value of `socket.timeout.ms` and remaining `message.timeout.ms` for the first message in the batch. Consumer: FetchRequests will use `fetch.wait.max.ms` + `socket.timeout.ms`. Admin: Admin requests will use `socket.timeout.ms` or explicitly set `rd_kafka_AdminOptions_set_operation_timeout()` value.
        /// </summary>
        public int SocketTimeoutMs { set { this.SetObject("socket.timeout.ms", value); } }

        /// <summary>
        ///     Maximum time a broker socket operation may block. A lower value improves responsiveness at the expense of slightly higher CPU usage. **Deprecated**
        /// </summary>
        public int SocketBlockingMaxMs { set { this.SetObject("socket.blocking.max.ms", value); } }

        /// <summary>
        ///     Broker socket send buffer size. System default is used if 0.
        /// </summary>
        public int SocketSendBufferBytes { set { this.SetObject("socket.send.buffer.bytes", value); } }

        /// <summary>
        ///     Broker socket receive buffer size. System default is used if 0.
        /// </summary>
        public int SocketReceiveBufferBytes { set { this.SetObject("socket.receive.buffer.bytes", value); } }

        /// <summary>
        ///     Enable TCP keep-alives (SO_KEEPALIVE) on broker sockets
        /// </summary>
        public bool SocketKeepaliveEnable { set { this.SetObject("socket.keepalive.enable", value); } }

        /// <summary>
        ///     Disable the Nagle algorithm (TCP_NODELAY) on broker sockets.
        /// </summary>
        public bool SocketNagleDisable { set { this.SetObject("socket.nagle.disable", value); } }

        /// <summary>
        ///     Disconnect from broker when this number of send failures (e.g., timed out requests) is reached. Disable with 0. WARNING: It is highly recommended to leave this setting at its default value of 1 to avoid the client and broker to become desynchronized in case of request timeouts. NOTE: The connection is automatically re-established.
        /// </summary>
        public int SocketMaxFails { set { this.SetObject("socket.max.fails", value); } }

        /// <summary>
        ///     How long to cache the broker address resolving results (milliseconds).
        /// </summary>
        public int BrokerAddressTtl { set { this.SetObject("broker.address.ttl", value); } }

        /// <summary>
        ///     Allowed broker IP address families: any, v4, v6
        /// </summary>
        public BrokerAddressFamilyType BrokerAddressFamily { set { this.SetObject("broker.address.family", value); } }

        /// <summary>
        ///     Throttle broker reconnection attempts by this value +-50%.
        /// </summary>
        public int ReconnectBackoffJitterMs { set { this.SetObject("reconnect.backoff.jitter.ms", value); } }

        /// <summary>
        ///     librdkafka statistics emit interval. The application also needs to register a stats callback using `rd_kafka_conf_set_stats_cb()`. The granularity is 1000ms. A value of 0 disables statistics.
        /// </summary>
        public int StatisticsIntervalMs { set { this.SetObject("statistics.interval.ms", value); } }

        /// <summary>
        ///     Logging level (syslog(3) levels)
        /// </summary>
        public int Log_level { set { this.SetObject("log_level", value); } }

        /// <summary>
        ///     Disable spontaneous log_cb from internal librdkafka threads, instead enqueue log messages on queue set with `rd_kafka_set_log_queue()` and serve log callbacks or events through the standard poll APIs. **NOTE**: Log messages will linger in a temporary queue until the log queue has been set.
        /// </summary>
        public bool LogQueue { set { this.SetObject("log.queue", value); } }

        /// <summary>
        ///     Print internal thread name in log messages (useful for debugging librdkafka internals)
        /// </summary>
        public bool LogThreadName { set { this.SetObject("log.thread.name", value); } }

        /// <summary>
        ///     Log broker disconnects. It might be useful to turn this off when interacting with 0.9 brokers with an aggressive `connection.max.idle.ms` value.
        /// </summary>
        public bool LogConnectionClose { set { this.SetObject("log.connection.close", value); } }

        /// <summary>
        ///     Signal that librdkafka will use to quickly terminate on rd_kafka_destroy(). If this signal is not set then there will be a delay before rd_kafka_wait_destroyed() returns true as internal threads are timing out their system calls. If this signal is set however the delay will be minimal. The application should mask this signal as an internal signal handler is installed.
        /// </summary>
        public int InternalTerminationSignal { set { this.SetObject("internal.termination.signal", value); } }

        /// <summary>
        ///     Request broker's supported API versions to adjust functionality to available protocol features. If set to false, or the ApiVersionRequest fails, the fallback version `broker.version.fallback` will be used. **NOTE**: Depends on broker version >=0.10.0. If the request is not supported by (an older) broker the `broker.version.fallback` fallback is used.
        /// </summary>
        public bool ApiVersionRequest { set { this.SetObject("api.version.request", value); } }

        /// <summary>
        ///     Timeout for broker API version requests.
        /// </summary>
        public int ApiVersionRequestTimeoutMs { set { this.SetObject("api.version.request.timeout.ms", value); } }

        /// <summary>
        ///     Dictates how long the `broker.version.fallback` fallback is used in the case the ApiVersionRequest fails. **NOTE**: The ApiVersionRequest is only issued when a new connection to the broker is made (such as after an upgrade).
        /// </summary>
        public int ApiVersionFallbackMs { set { this.SetObject("api.version.fallback.ms", value); } }

        /// <summary>
        ///     Older broker versions (before 0.10.0) provide no way for a client to query for supported protocol features (ApiVersionRequest, see `api.version.request`) making it impossible for the client to know what features it may use. As a workaround a user may set this property to the expected broker version and the client will automatically adjust its feature set accordingly if the ApiVersionRequest fails (or is disabled). The fallback broker version will be used for `api.version.fallback.ms`. Valid values are: 0.9.0, 0.8.2, 0.8.1, 0.8.0. Any other value, such as 0.10.2.1, enables ApiVersionRequests.
        /// </summary>
        public string BrokerVersionFallback { set { this.SetObject("broker.version.fallback", value); } }

        /// <summary>
        ///     Protocol used to communicate with brokers.
        /// </summary>
        public SecurityProtocolType SecurityProtocol { set { this.SetObject("security.protocol", value); } }

        /// <summary>
        ///     A cipher suite is a named combination of authentication, encryption, MAC and key exchange algorithm used to negotiate the security settings for a network connection using TLS or SSL network protocol. See manual page for `ciphers(1)` and `SSL_CTX_set_cipher_list(3).
        /// </summary>
        public string SslCipherSuites { set { this.SetObject("ssl.cipher.suites", value); } }

        /// <summary>
        ///     The supported-curves extension in the TLS ClientHello message specifies the curves (standard/named, or 'explicit' GF(2^k) or GF(p)) the client is willing to have the server use. See manual page for `SSL_CTX_set1_curves_list(3)`. OpenSSL >= 1.0.2 required.
        /// </summary>
        public string SslCurvesList { set { this.SetObject("ssl.curves.list", value); } }

        /// <summary>
        ///     The client uses the TLS ClientHello signature_algorithms extension to indicate to the server which signature/hash algorithm pairs may be used in digital signatures. See manual page for `SSL_CTX_set1_sigalgs_list(3)`. OpenSSL >= 1.0.2 required.
        /// </summary>
        public string SslSigalgsList { set { this.SetObject("ssl.sigalgs.list", value); } }

        /// <summary>
        ///     Path to client's private key (PEM) used for authentication.
        /// </summary>
        public string SslKeyLocation { set { this.SetObject("ssl.key.location", value); } }

        /// <summary>
        ///     Private key passphrase
        /// </summary>
        public string SslKeyPassword { set { this.SetObject("ssl.key.password", value); } }

        /// <summary>
        ///     Path to client's public key (PEM) used for authentication.
        /// </summary>
        public string SslCertificateLocation { set { this.SetObject("ssl.certificate.location", value); } }

        /// <summary>
        ///     File or directory path to CA certificate(s) for verifying the broker's key.
        /// </summary>
        public string SslCaLocation { set { this.SetObject("ssl.ca.location", value); } }

        /// <summary>
        ///     Path to CRL for verifying broker's certificate validity.
        /// </summary>
        public string SslCrlLocation { set { this.SetObject("ssl.crl.location", value); } }

        /// <summary>
        ///     Path to client's keystore (PKCS#12) used for authentication.
        /// </summary>
        public string SslKeystoreLocation { set { this.SetObject("ssl.keystore.location", value); } }

        /// <summary>
        ///     Client's keystore (PKCS#12) password.
        /// </summary>
        public string SslKeystorePassword { set { this.SetObject("ssl.keystore.password", value); } }

        /// <summary>
        ///     SASL mechanism to use for authentication. Supported: GSSAPI, PLAIN, SCRAM-SHA-256, SCRAM-SHA-512. **NOTE**: Despite the name only one mechanism must be configured.
        /// </summary>
        public string SaslMechanism { set { this.SetObject("sasl.mechanism", value); } }

        /// <summary>
        ///     Kerberos principal name that Kafka runs as, not including /hostname@REALM
        /// </summary>
        public string SaslKerberosServiceName { set { this.SetObject("sasl.kerberos.service.name", value); } }

        /// <summary>
        ///     This client's Kerberos principal name. (Not supported on Windows, will use the logon user's principal).
        /// </summary>
        public string SaslKerberosPrincipal { set { this.SetObject("sasl.kerberos.principal", value); } }

        /// <summary>
        ///     Full kerberos kinit command string, %{config.prop.name} is replaced by corresponding config object value, %{broker.name} returns the broker's hostname.
        /// </summary>
        public string SaslKerberosKinitCmd { set { this.SetObject("sasl.kerberos.kinit.cmd", value); } }

        /// <summary>
        ///     Path to Kerberos keytab file. Uses system default if not set.**NOTE**: This is not automatically used but must be added to the template in sasl.kerberos.kinit.cmd as ` ... -t %{sasl.kerberos.keytab}`.
        /// </summary>
        public string SaslKerberosKeytab { set { this.SetObject("sasl.kerberos.keytab", value); } }

        /// <summary>
        ///     Minimum time in milliseconds between key refresh attempts.
        /// </summary>
        public int SaslKerberosMinTimeBeforeRelogin { set { this.SetObject("sasl.kerberos.min.time.before.relogin", value); } }

        /// <summary>
        ///     SASL username for use with the PLAIN and SASL-SCRAM-.. mechanisms
        /// </summary>
        public string SaslUsername { set { this.SetObject("sasl.username", value); } }

        /// <summary>
        ///     SASL password for use with the PLAIN and SASL-SCRAM-.. mechanism
        /// </summary>
        public string SaslPassword { set { this.SetObject("sasl.password", value); } }

        /// <summary>
        ///     List of plugin libraries to load (; separated). The library search path is platform dependent (see dlopen(3) for Unix and LoadLibrary() for Windows). If no filename extension is specified the platform-specific extension (such as .dll or .so) will be appended automatically.
        /// </summary>
        public string PluginLibraryPaths { set { this.SetObject("plugin.library.paths", value); } }

        /// <summary>
        ///     Client group id string. All clients sharing the same group.id belong to the same group.
        /// </summary>
        public string GroupId { set { this.SetObject("group.id", value); } }

        /// <summary>
        ///     Name of partition assignment strategy to use when elected group leader assigns partitions to group members.
        /// </summary>
        public string PartitionAssignmentStrategy { set { this.SetObject("partition.assignment.strategy", value); } }

        /// <summary>
        ///     Client group session and failure detection timeout.
        /// </summary>
        public int SessionTimeoutMs { set { this.SetObject("session.timeout.ms", value); } }

        /// <summary>
        ///     Group session keepalive heartbeat interval.
        /// </summary>
        public int HeartbeatIntervalMs { set { this.SetObject("heartbeat.interval.ms", value); } }

        /// <summary>
        ///     Group protocol type
        /// </summary>
        public string GroupProtocolType { set { this.SetObject("group.protocol.type", value); } }

        /// <summary>
        ///     How often to query for the current client group coordinator. If the currently assigned coordinator is down the configured query interval will be divided by ten to more quickly recover in case of coordinator reassignment.
        /// </summary>
        public int CoordinatorQueryIntervalMs { set { this.SetObject("coordinator.query.interval.ms", value); } }

        /// <summary>
        ///     Specifies a delegate for handling log messages. If not specified,
        ///     a default callback that writes to stderr will be used.
        /// </summary>
        /// <remarks>
        ///     By default not many log messages are generated.
        ///
        ///     For more verbose logging, specify one or more debug contexts
        ///     using the 'debug' configuration property. The 'log_level'
        ///     configuration property is also relevant, however logging is
        ///     verbose by default given a debug context has been specified,
        ///     so you typically shouldn't adjust this value.
        ///
        ///     Warning: Log handlers are called spontaneously from internal
        ///     librdkafka threads and the application must not call any
        ///     Confluent.Kafka APIs from within a log handler or perform any
        ///     prolonged operations.
        /// </remarks>
        public Action<LogMessage> LogCallback { get; set; }

        /// <summary>
        ///     Specifies a delegate for handling error events e.g. connection
        ///     failures or all brokers down. Note that the client will try
        ///     to automatically recover from errors - these errors should be
        ///     seen as informational rather than catastrophic.
        /// </summary>
        /// <remarks>
        ///     On the Consumer, executes as a side-effect of
        ///     <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Consume(System.Threading.CancellationToken)" />
        ///     (on the same thread) and on the Producer and AdminClient, on the
        ///     background poll thread.
        /// </remarks>
        public Action<ErrorEvent> ErrorCallback { get; set; }

        /// <summary>
        ///     Specifies a delegate for handling statistics events - a JSON
        ///     formatted string as defined here:
        ///     https://github.com/edenhill/librdkafka/wiki/Statistics
        /// </summary>
        /// <remarks>
        ///     You can enable statistics and set the statistics interval
        ///     using the statistics.interval.ms configuration parameter
        ///     (disabled by default).
        ///
        ///     On the Consumer, executes as a side-effect of
        ///     <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Consume(System.Threading.CancellationToken)" />
        ///     (on the same thread) and on the Producer and AdminClient, on the
        ///     background poll thread.
        /// </remarks>
        public Action<string> StatsCallback { get; set; }

        /// <summary>
        ///     Set a configuration property using a string key / value pair.
        /// </summary>
        /// <remarks>
        ///     Two scenarios where this is useful: 1. For setting librdkafka
        ///     plugin config properties. 2. You are using a different version of 
        ///     librdkafka to the one provided as a dependency of the Confluent.Kafka
        ///     package and the configuration properties have evolved.
        /// </remarks>
        /// <param name="key">
        ///     The configuration property name.
        /// </param>
        /// <param name="val">
        ///     The property value.
        /// </param>
        public void Set(string key, string val)
        {
            this.properties[key] = val;
        }

        /// <summary>
        ///     Set a configuration property using a key / value pair (null checked).
        /// </summary>
        protected void SetObject(string name, object val)
        {
            if (val == null)
            {
                throw new ArgumentException($"value for property {name} cannot be null.");
            }
            this.properties[name] = val.ToString();
        }

        /// <summary>
        ///     The configuration properties.
        /// </summary>
        protected Dictionary<string, string> properties = new Dictionary<string, string>();

        /// <summary>
        ///     	Returns an enumerator that iterates through the property collection.
        /// </summary>
        /// <returns>
        ///         An enumerator that iterates through the property collection.
        /// </returns>
        public IEnumerator<KeyValuePair<string, string>> GetEnumerator() => this.properties.GetEnumerator();

        /// <summary>
        ///     	Returns an enumerator that iterates through the property collection.
        /// </summary>
        /// <returns>
        ///         An enumerator that iterates through the property collection.
        /// </returns>
        IEnumerator IEnumerable.GetEnumerator() => this.properties.GetEnumerator();
    }


    /// <summary>
    ///     AdminClient configuration properties
    /// </summary>
    public class AdminClientConfig : ClientConfig
    {
        /// <summary>
        ///     Initialize a new empty <see cref="AdminClientConfig" /> instance.
        /// </summary>
        public AdminClientConfig() {}

        /// <summary>
        ///     Initialize a new <see cref="AdminClientConfig" /> instance based on
        ///     an existing <see cref="ClientConfig" /> instance.
        /// </summary>
        public AdminClientConfig(ClientConfig config) { this.properties = new Dictionary<string, string>(config.ToDictionary(a => a.Key, a => a.Value)); }

        /// <summary>
        ///     Initialize a new <see cref="AdminClientConfig" /> instance based on
        ///     an existing key/value pair collection.
        /// </summary>
        public AdminClientConfig(IEnumerable<KeyValuePair<string, string>> config) { this.properties = new Dictionary<string, string>(config.ToDictionary(a => a.Key, a => a.Value)); }
    }


    /// <summary>
    ///     Producer configuration properties
    /// </summary>
    public class ProducerConfig : ClientConfig
    {
        /// <summary>
        ///     Initialize a new empty <see cref="ProducerConfig" /> instance.
        /// </summary>
        public ProducerConfig() {}

        /// <summary>
        ///     Initialize a new <see cref="ProducerConfig" /> instance based on
        ///     an existing <see cref="ClientConfig" /> instance.
        /// </summary>
        public ProducerConfig(ClientConfig config) { this.properties = new Dictionary<string, string>(config.ToDictionary(a => a.Key, a => a.Value)); }

        /// <summary>
        ///     Initialize a new <see cref="ProducerConfig" /> instance based on
        ///     an existing key/value pair collection.
        /// </summary>
        public ProducerConfig(IEnumerable<KeyValuePair<string, string>> config) { this.properties = new Dictionary<string, string>(config.ToDictionary(a => a.Key, a => a.Value)); }

        /// <summary>
        ///     Specifies whether or not the producer should start a background poll 
        ///     thread to receive delivery reports and event notifications. Generally,
        ///     this should be set to true. If set to false, you will need to call 
        ///     the Poll function manually.
        /// 
        ///     default: true
        /// </summary>
        public bool EnableBackgroundPoll { set { this.properties["dotnet.producer.enable.background.poll"] = value.ToString(); } }

        /// <summary>
        ///     Specifies whether to enable notification of delivery reports. Typically
        ///     you should set this parameter to true. Set it to false for "fire and
        ///     forget" semantics and a small boost in performance.
        /// 
        ///     default: true
        /// </summary>
        public bool EnableDeliveryReports { set { this.properties["dotnet.producer.enable.delivery.reports"] = value.ToString(); } }

        /// <summary>
        ///     A comma separated list of fields that may be optionally set in delivery
        ///     reports. Disabling delivery report fields that you do not require will
        ///     improve maximum throughput and reduce memory usage. Allowed values:
        ///     key, value, timestamp, headers, all, none.
        /// 
        ///     default: all
        /// </summary>
        public string DeliveryReportFields { set { this.properties["dotnet.producer.delivery.report.fields"] = value.ToString(); } }

        /// <summary>
        ///     Maximum number of messages allowed on the producer queue.
        /// </summary>
        public int QueueBufferingMaxMessages { set { this.SetObject("queue.buffering.max.messages", value); } }

        /// <summary>
        ///     Maximum total message size sum allowed on the producer queue. This property has higher priority than queue.buffering.max.messages.
        /// </summary>
        public int QueueBufferingMaxKbytes { set { this.SetObject("queue.buffering.max.kbytes", value); } }

        /// <summary>
        ///     Delay in milliseconds to wait for messages in the producer queue to accumulate before constructing message batches (MessageSets) to transmit to brokers. A higher value allows larger and more effective (less overhead, improved compression) batches of messages to accumulate at the expense of increased message delivery latency.
        /// </summary>
        public int LingerMs { set { this.SetObject("linger.ms", value); } }

        /// <summary>
        ///     How many times to retry sending a failing MessageSet. **Note:** retrying may cause reordering.
        /// </summary>
        public int MessageSendMaxRetries { set { this.SetObject("message.send.max.retries", value); } }

        /// <summary>
        ///     The backoff time in milliseconds before retrying a protocol request.
        /// </summary>
        public int RetryBackoffMs { set { this.SetObject("retry.backoff.ms", value); } }

        /// <summary>
        ///     The threshold of outstanding not yet transmitted broker requests needed to backpressure the producer's message accumulator. If the number of not yet transmitted requests equals or exceeds this number, produce request creation that would have otherwise been triggered (for example, in accordance with linger.ms) will be delayed. A lower number yields larger and more effective batches. A higher value can improve latency when using compression on slow machines.
        /// </summary>
        public int QueueBufferingBackpressureThreshold { set { this.SetObject("queue.buffering.backpressure.threshold", value); } }

        /// <summary>
        ///     Maximum number of messages batched in one MessageSet. The total MessageSet size is also limited by message.max.bytes.
        /// </summary>
        public int BatchNumMessages { set { this.SetObject("batch.num.messages", value); } }

        /// <summary>
        ///     This field indicates how many acknowledgements the leader broker must receive from ISR brokers before responding to the request: *0*=Broker does not send any response/ack to client, *1*=Only the leader broker will need to ack the message, *-1* or *all*=broker will block until message is committed by all in sync replicas (ISRs) or broker's `min.insync.replicas` setting before sending response.
        /// </summary>
        public int Acks { set { this.SetObject("acks", value); } }

        /// <summary>
        ///     The ack timeout of the producer request in milliseconds. This value is only enforced by the broker and relies on `request.required.acks` being != 0.
        /// </summary>
        public int RequestTimeoutMs { set { this.SetObject("request.timeout.ms", value); } }

        /// <summary>
        ///     Local message timeout. This value is only enforced locally and limits the time a produced message waits for successful delivery. A time of 0 is infinite. This is the maximum time librdkafka may use to deliver a message (including retries). Delivery error occurs when either the retry count or the message timeout are exceeded.
        /// </summary>
        public int MessageTimeoutMs { set { this.SetObject("message.timeout.ms", value); } }

        /// <summary>
        ///     Producer queuing strategy. FIFO preserves produce ordering, while LIFO prioritizes new messages. WARNING: `lifo` is experimental and subject to change or removal.
        /// </summary>
        public QueuingStrategyType QueuingStrategy { set { this.SetObject("queuing.strategy", value); } }

        /// <summary>
        ///     Partitioner: `random` - random distribution, `consistent` - CRC32 hash of key (Empty and NULL keys are mapped to single partition), `consistent_random` - CRC32 hash of key (Empty and NULL keys are randomly partitioned), `murmur2` - Java Producer compatible Murmur2 hash of key (NULL keys are mapped to single partition), `murmur2_random` - Java Producer compatible Murmur2 hash of key (NULL keys are randomly partitioned. This is functionally equivalent to the default partitioner in the Java Producer.).
        /// </summary>
        public string Partitioner { set { this.SetObject("partitioner", value); } }

        /// <summary>
        ///     Compression level parameter for algorithm selected by configuration property `compression.codec`. Higher values will result in better compression at the cost of more CPU usage. Usable range is algorithm-dependent: [0-9] for gzip; [0-12] for lz4; only 0 for snappy; -1 = codec-dependent default compression level.
        /// </summary>
        public int CompressionLevel { set { this.SetObject("compression.level", value); } }

    }


    /// <summary>
    ///     Consumer configuration properties
    /// </summary>
    public class ConsumerConfig : ClientConfig
    {
        /// <summary>
        ///     Initialize a new empty <see cref="ConsumerConfig" /> instance.
        /// </summary>
        public ConsumerConfig() {}

        /// <summary>
        ///     Initialize a new <see cref="ConsumerConfig" /> instance based on
        ///     an existing <see cref="ClientConfig" /> instance.
        /// </summary>
        public ConsumerConfig(ClientConfig config) { this.properties = new Dictionary<string, string>(config.ToDictionary(a => a.Key, a => a.Value)); }

        /// <summary>
        ///     Initialize a new <see cref="ConsumerConfig" /> instance based on
        ///     an existing key/value pair collection.
        /// </summary>
        public ConsumerConfig(IEnumerable<KeyValuePair<string, string>> config) { this.properties = new Dictionary<string, string>(config.ToDictionary(a => a.Key, a => a.Value)); }

        /// <summary>
        ///     A comma separated list of fields that may be optionally set
        ///     in <see cref="Confluent.Kafka.ConsumeResult{TKey, TValue}" />
        ///     objects returned by the
        ///     <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Consume(System.TimeSpan)" />
        ///     method. Disabling fields that you do not require will improve 
        ///     throughput and reduce memory consumption. Allowed values:
        ///     headers, timestamp, topic, all, none
        /// 
        ///     default: all
        /// </summary>
        public string ConsumeResultFields { set { this.properties["dotnet.consumer.consume.result.fields"] = value; } }

        /// <summary>
        ///     Automatically and periodically commit offsets in the background. Note: setting this to false does not prevent the consumer from fetching previously committed start offsets. To circumvent this behaviour set specific start offsets per partition in the call to assign().
        /// </summary>
        public bool EnableAutoCommit { set { this.SetObject("enable.auto.commit", value); } }

        /// <summary>
        ///     The frequency in milliseconds that the consumer offsets are committed (written) to offset storage. (0 = disable). This setting is used by the high-level consumer.
        /// </summary>
        public int AutoCommitIntervalMs { set { this.SetObject("auto.commit.interval.ms", value); } }

        /// <summary>
        ///     Automatically store offset of last message provided to application.
        /// </summary>
        public bool EnableAutoOffsetStore { set { this.SetObject("enable.auto.offset.store", value); } }

        /// <summary>
        ///     Minimum number of messages per topic+partition librdkafka tries to maintain in the local consumer queue.
        /// </summary>
        public int QueuedMinMessages { set { this.SetObject("queued.min.messages", value); } }

        /// <summary>
        ///     Maximum number of kilobytes per topic+partition in the local consumer queue. This value may be overshot by fetch.message.max.bytes. This property has higher priority than queued.min.messages.
        /// </summary>
        public int QueuedMaxMessagesKbytes { set { this.SetObject("queued.max.messages.kbytes", value); } }

        /// <summary>
        ///     Maximum time the broker may wait to fill the response with fetch.min.bytes.
        /// </summary>
        public int FetchWaitMaxMs { set { this.SetObject("fetch.wait.max.ms", value); } }

        /// <summary>
        ///     Initial maximum number of bytes per topic+partition to request when fetching messages from the broker. If the client encounters a message larger than this value it will gradually try to increase it until the entire message can be fetched.
        /// </summary>
        public int MaxPartitionFetchBytes { set { this.SetObject("max.partition.fetch.bytes", value); } }

        /// <summary>
        ///     Maximum amount of data the broker shall return for a Fetch request. Messages are fetched in batches by the consumer and if the first message batch in the first non-empty partition of the Fetch request is larger than this value, then the message batch will still be returned to ensure the consumer can make progress. The maximum message batch size accepted by the broker is defined via `message.max.bytes` (broker config) or `max.message.bytes` (broker topic config). `fetch.max.bytes` is automatically adjusted upwards to be at least `message.max.bytes` (consumer config).
        /// </summary>
        public int FetchMaxBytes { set { this.SetObject("fetch.max.bytes", value); } }

        /// <summary>
        ///     Minimum number of bytes the broker responds with. If fetch.wait.max.ms expires the accumulated data will be sent to the client regardless of this setting.
        /// </summary>
        public int FetchMinBytes { set { this.SetObject("fetch.min.bytes", value); } }

        /// <summary>
        ///     How long to postpone the next fetch request for a topic+partition in case of a fetch error.
        /// </summary>
        public int FetchErrorBackoffMs { set { this.SetObject("fetch.error.backoff.ms", value); } }

        /// <summary>
        ///     Emit RD_KAFKA_RESP_ERR__PARTITION_EOF event whenever the consumer reaches the end of a partition.
        /// </summary>
        public bool EnablePartitionEof { set { this.SetObject("enable.partition.eof", value); } }

        /// <summary>
        ///     Verify CRC32 of consumed messages, ensuring no on-the-wire or on-disk corruption to the messages occurred. This check comes at slightly increased CPU usage.
        /// </summary>
        public bool CheckCrcs { set { this.SetObject("check.crcs", value); } }

        /// <summary>
        ///     Action to take when there is no initial offset in offset store or the desired offset is out of range: 'smallest','earliest' - automatically reset the offset to the smallest offset, 'largest','latest' - automatically reset the offset to the largest offset, 'error' - trigger an error which is retrieved by consuming messages and checking 'message->err'.
        /// </summary>
        public AutoOffsetResetType AutoOffsetReset { set { this.SetObject("auto.offset.reset", value); } }

    }

}
