// *** Auto-generated from librdkafka branch v1.0.0-RC5 *** - do not modify manually.
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
    public enum BrokerAddressFamily
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
    public enum SecurityProtocol
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
        ///     SaslPlaintext
        /// </summary>
        SaslPlaintext,

        /// <summary>
        ///     SaslSsl
        /// </summary>
        SaslSsl
    }

    /// <summary>
    ///     PartitionAssignmentStrategy enum values
    /// </summary>
    public enum PartitionAssignmentStrategy
    {
        /// <summary>
        ///     Range
        /// </summary>
        Range,

        /// <summary>
        ///     RoundRobin
        /// </summary>
        RoundRobin
    }

    /// <summary>
    ///     Partitioner enum values
    /// </summary>
    public enum Partitioner
    {
        /// <summary>
        ///     Random
        /// </summary>
        Random,

        /// <summary>
        ///     Consistent
        /// </summary>
        Consistent,

        /// <summary>
        ///     ConsistentRandom
        /// </summary>
        ConsistentRandom,

        /// <summary>
        ///     Murmur2
        /// </summary>
        Murmur2,

        /// <summary>
        ///     Murmur2Random
        /// </summary>
        Murmur2Random
    }

    /// <summary>
    ///     AutoOffsetReset enum values
    /// </summary>
    public enum AutoOffsetReset
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
    ///     SaslMechanism enum values
    /// </summary>
    public enum SaslMechanism
    {
        /// <summary>
        ///     GSSAPI
        /// </summary>
        Gssapi,

        /// <summary>
        ///     PLAIN
        /// </summary>
        Plain,

        /// <summary>
        ///     SCRAM-SHA-256
        /// </summary>
        ScramSha256,

        /// <summary>
        ///     SCRAM-SHA-512
        /// </summary>
        ScramSha512
    }

    /// <summary>
    ///     Acks enum values
    /// </summary>
    public enum Acks : int
    {
        /// <summary>
        ///     None
        /// </summary>
        None = 0,

        /// <summary>
        ///     Leader
        /// </summary>
        Leader = 1,

        /// <summary>
        ///     All
        /// </summary>
        All = -1
    }

    /// <summary>
    ///     Configuration common to all clients
    /// </summary>
    public class ClientConfig : Config
    {

        /// <summary>
        ///     SASL mechanism to use for authentication. Supported: GSSAPI, PLAIN, SCRAM-SHA-256, SCRAM-SHA-512. **NOTE**: Despite the name, you may not configure more than one mechanism.
        /// </summary>
        public SaslMechanism? SaslMechanism
        {
            get
            {
                var r = Get("sasl.mechanism");
                if (r == null) { return null; }
                if (r == "GSSAPI") { return Confluent.Kafka.SaslMechanism.Gssapi; }
                if (r == "PLAIN") { return Confluent.Kafka.SaslMechanism.Plain; }
                if (r == "SCRAM-SHA-256") { return Confluent.Kafka.SaslMechanism.ScramSha256; }
                if (r == "SCRAM-SHA-512") { return Confluent.Kafka.SaslMechanism.ScramSha512; }
                throw new ArgumentException($"Unknown sasl.mechanism value {r}");
            }
            set
            {
                if (value == null) { this.properties.Remove("sasl.mechanism"); }
                else if (value == Confluent.Kafka.SaslMechanism.Gssapi) { this.properties["sasl.mechanism"] = "GSSAPI"; }
                else if (value == Confluent.Kafka.SaslMechanism.Plain) { this.properties["sasl.mechanism"] = "PLAIN"; }
                else if (value == Confluent.Kafka.SaslMechanism.ScramSha256) { this.properties["sasl.mechanism"] = "SCRAM-SHA-256"; }
                else if (value == Confluent.Kafka.SaslMechanism.ScramSha512) { this.properties["sasl.mechanism"] = "SCRAM-SHA-512"; }
                else throw new NotImplementedException($"Unknown sasl.mechanism value {value}");
            }
        }


        /// <summary>
        ///     This field indicates the number of acknowledgements the leader broker must receive from ISR brokers
        ///     before responding to the request: Zero=Broker does not send any response/ack to client, One=The
        ///     leader will write the record to its local log but will respond without awaiting full acknowledgement
        ///     from all followers. All=Broker will block until message is committed by all in sync replicas (ISRs).
        ///     If there are less than min.insync.replicas (broker configuration) in the ISR set the produce request
        ///     will fail.
        /// </summary>
        public Acks? Acks
        {
            get
            {
                var r = Get("acks");
                if (r == null) { return null; }
                if (r == "0") { return Confluent.Kafka.Acks.None; }
                if (r == "1") { return Confluent.Kafka.Acks.Leader; }
                if (r == "-1" || r == "all") { return Confluent.Kafka.Acks.All; }
                return (Acks)(int.Parse(r));
            }
            set
            {
                if (value == null) { this.properties.Remove("acks"); }
                else if (value == Confluent.Kafka.Acks.None) { this.properties["acks"] = "0"; }
                else if (value == Confluent.Kafka.Acks.Leader) { this.properties["acks"] = "1"; }
                else if (value == Confluent.Kafka.Acks.All) { this.properties["acks"] = "-1"; }
                else { this.properties["acks"] = ((int)value.Value).ToString(); }
            }
        }

        /// <summary>
        ///     Client identifier.
        ///
        ///     default: rdkafka
        ///     importance: low
        /// </summary>
        public string ClientId { get { return Get("client.id"); } set { this.SetObject("client.id", value); } }

        /// <summary>
        ///     Initial list of brokers as a CSV list of broker host or host:port. The application may also use `rd_kafka_brokers_add()` to add brokers during runtime.
        ///
        ///     default: ''
        ///     importance: high
        /// </summary>
        public string BootstrapServers { get { return Get("bootstrap.servers"); } set { this.SetObject("bootstrap.servers", value); } }

        /// <summary>
        ///     Maximum Kafka protocol request message size.
        ///
        ///     default: 1000000
        ///     importance: medium
        /// </summary>
        public int? MessageMaxBytes { get { return GetInt("message.max.bytes"); } set { this.SetObject("message.max.bytes", value); } }

        /// <summary>
        ///     Maximum size for message to be copied to buffer. Messages larger than this will be passed by reference (zero-copy) at the expense of larger iovecs.
        ///
        ///     default: 65535
        ///     importance: low
        /// </summary>
        public int? MessageCopyMaxBytes { get { return GetInt("message.copy.max.bytes"); } set { this.SetObject("message.copy.max.bytes", value); } }

        /// <summary>
        ///     Maximum Kafka protocol response message size. This serves as a safety precaution to avoid memory exhaustion in case of protocol hickups. This value must be at least `fetch.max.bytes`  + 512 to allow for protocol overhead; the value is adjusted automatically unless the configuration property is explicitly set.
        ///
        ///     default: 100000000
        ///     importance: medium
        /// </summary>
        public int? ReceiveMessageMaxBytes { get { return GetInt("receive.message.max.bytes"); } set { this.SetObject("receive.message.max.bytes", value); } }

        /// <summary>
        ///     Maximum number of in-flight requests per broker connection. This is a generic property applied to all broker communication, however it is primarily relevant to produce requests. In particular, note that other mechanisms limit the number of outstanding consumer fetch request per broker to one.
        ///
        ///     default: 1000000
        ///     importance: low
        /// </summary>
        public int? MaxInFlight { get { return GetInt("max.in.flight"); } set { this.SetObject("max.in.flight", value); } }

        /// <summary>
        ///     Non-topic request timeout in milliseconds. This is for metadata requests, etc.
        ///
        ///     default: 60000
        ///     importance: low
        /// </summary>
        public int? MetadataRequestTimeoutMs { get { return GetInt("metadata.request.timeout.ms"); } set { this.SetObject("metadata.request.timeout.ms", value); } }

        /// <summary>
        ///     Topic metadata refresh interval in milliseconds. The metadata is automatically refreshed on error and connect. Use -1 to disable the intervalled refresh.
        ///
        ///     default: 300000
        ///     importance: low
        /// </summary>
        public int? TopicMetadataRefreshIntervalMs { get { return GetInt("topic.metadata.refresh.interval.ms"); } set { this.SetObject("topic.metadata.refresh.interval.ms", value); } }

        /// <summary>
        ///     Metadata cache max age. Defaults to topic.metadata.refresh.interval.ms * 3
        ///
        ///     default: 900000
        ///     importance: low
        /// </summary>
        public int? MetadataMaxAgeMs { get { return GetInt("metadata.max.age.ms"); } set { this.SetObject("metadata.max.age.ms", value); } }

        /// <summary>
        ///     When a topic loses its leader a new metadata request will be enqueued with this initial interval, exponentially increasing until the topic metadata has been refreshed. This is used to recover quickly from transitioning leader brokers.
        ///
        ///     default: 250
        ///     importance: low
        /// </summary>
        public int? TopicMetadataRefreshFastIntervalMs { get { return GetInt("topic.metadata.refresh.fast.interval.ms"); } set { this.SetObject("topic.metadata.refresh.fast.interval.ms", value); } }

        /// <summary>
        ///     Sparse metadata requests (consumes less network bandwidth)
        ///
        ///     default: true
        ///     importance: low
        /// </summary>
        public bool? TopicMetadataRefreshSparse { get { return GetBool("topic.metadata.refresh.sparse"); } set { this.SetObject("topic.metadata.refresh.sparse", value); } }

        /// <summary>
        ///     Topic blacklist, a comma-separated list of regular expressions for matching topic names that should be ignored in broker metadata information as if the topics did not exist.
        ///
        ///     default: ''
        ///     importance: low
        /// </summary>
        public string TopicBlacklist { get { return Get("topic.blacklist"); } set { this.SetObject("topic.blacklist", value); } }

        /// <summary>
        ///     A comma-separated list of debug contexts to enable. Detailed Producer debugging: broker,topic,msg. Consumer: consumer,cgrp,topic,fetch
        ///
        ///     default: ''
        ///     importance: medium
        /// </summary>
        public string Debug { get { return Get("debug"); } set { this.SetObject("debug", value); } }

        /// <summary>
        ///     Default timeout for network requests. Producer: ProduceRequests will use the lesser value of `socket.timeout.ms` and remaining `message.timeout.ms` for the first message in the batch. Consumer: FetchRequests will use `fetch.wait.max.ms` + `socket.timeout.ms`. Admin: Admin requests will use `socket.timeout.ms` or explicitly set `rd_kafka_AdminOptions_set_operation_timeout()` value.
        ///
        ///     default: 60000
        ///     importance: low
        /// </summary>
        public int? SocketTimeoutMs { get { return GetInt("socket.timeout.ms"); } set { this.SetObject("socket.timeout.ms", value); } }

        /// <summary>
        ///     Broker socket send buffer size. System default is used if 0.
        ///
        ///     default: 0
        ///     importance: low
        /// </summary>
        public int? SocketSendBufferBytes { get { return GetInt("socket.send.buffer.bytes"); } set { this.SetObject("socket.send.buffer.bytes", value); } }

        /// <summary>
        ///     Broker socket receive buffer size. System default is used if 0.
        ///
        ///     default: 0
        ///     importance: low
        /// </summary>
        public int? SocketReceiveBufferBytes { get { return GetInt("socket.receive.buffer.bytes"); } set { this.SetObject("socket.receive.buffer.bytes", value); } }

        /// <summary>
        ///     Enable TCP keep-alives (SO_KEEPALIVE) on broker sockets
        ///
        ///     default: false
        ///     importance: low
        /// </summary>
        public bool? SocketKeepaliveEnable { get { return GetBool("socket.keepalive.enable"); } set { this.SetObject("socket.keepalive.enable", value); } }

        /// <summary>
        ///     Disable the Nagle algorithm (TCP_NODELAY) on broker sockets.
        ///
        ///     default: false
        ///     importance: low
        /// </summary>
        public bool? SocketNagleDisable { get { return GetBool("socket.nagle.disable"); } set { this.SetObject("socket.nagle.disable", value); } }

        /// <summary>
        ///     Disconnect from broker when this number of send failures (e.g., timed out requests) is reached. Disable with 0. WARNING: It is highly recommended to leave this setting at its default value of 1 to avoid the client and broker to become desynchronized in case of request timeouts. NOTE: The connection is automatically re-established.
        ///
        ///     default: 1
        ///     importance: low
        /// </summary>
        public int? SocketMaxFails { get { return GetInt("socket.max.fails"); } set { this.SetObject("socket.max.fails", value); } }

        /// <summary>
        ///     How long to cache the broker address resolving results (milliseconds).
        ///
        ///     default: 1000
        ///     importance: low
        /// </summary>
        public int? BrokerAddressTtl { get { return GetInt("broker.address.ttl"); } set { this.SetObject("broker.address.ttl", value); } }

        /// <summary>
        ///     Allowed broker IP address families: any, v4, v6
        ///
        ///     default: any
        ///     importance: low
        /// </summary>
        public BrokerAddressFamily? BrokerAddressFamily { get { return (BrokerAddressFamily?)GetEnum(typeof(BrokerAddressFamily), "broker.address.family"); } set { this.SetObject("broker.address.family", value); } }

        /// <summary>
        ///     When enabled the client will only connect to brokers it needs to communicate with. When disabled the client will maintain connections to all brokers in the cluster.
        ///
        ///     default: true
        ///     importance: medium
        /// </summary>
        public bool? EnableSparseConnections { get { return GetBool("enable.sparse.connections"); } set { this.SetObject("enable.sparse.connections", value); } }

        /// <summary>
        ///     The initial time to wait before reconnecting to a broker after the connection has been closed. The time is increased exponentially until `reconnect.backoff.max.ms` is reached. -25% to +50% jitter is applied to each reconnect backoff. A value of 0 disables the backoff and reconnects immediately.
        ///
        ///     default: 100
        ///     importance: medium
        /// </summary>
        public int? ReconnectBackoffMs { get { return GetInt("reconnect.backoff.ms"); } set { this.SetObject("reconnect.backoff.ms", value); } }

        /// <summary>
        ///     The maximum time to wait before reconnecting to a broker after the connection has been closed.
        ///
        ///     default: 10000
        ///     importance: medium
        /// </summary>
        public int? ReconnectBackoffMaxMs { get { return GetInt("reconnect.backoff.max.ms"); } set { this.SetObject("reconnect.backoff.max.ms", value); } }

        /// <summary>
        ///     librdkafka statistics emit interval. The application also needs to register a stats callback using `rd_kafka_conf_set_stats_cb()`. The granularity is 1000ms. A value of 0 disables statistics.
        ///
        ///     default: 0
        ///     importance: high
        /// </summary>
        public int? StatisticsIntervalMs { get { return GetInt("statistics.interval.ms"); } set { this.SetObject("statistics.interval.ms", value); } }

        /// <summary>
        ///     Disable spontaneous log_cb from internal librdkafka threads, instead enqueue log messages on queue set with `rd_kafka_set_log_queue()` and serve log callbacks or events through the standard poll APIs. **NOTE**: Log messages will linger in a temporary queue until the log queue has been set.
        ///
        ///     default: false
        ///     importance: low
        /// </summary>
        public bool? LogQueue { get { return GetBool("log.queue"); } set { this.SetObject("log.queue", value); } }

        /// <summary>
        ///     Print internal thread name in log messages (useful for debugging librdkafka internals)
        ///
        ///     default: true
        ///     importance: low
        /// </summary>
        public bool? LogThreadName { get { return GetBool("log.thread.name"); } set { this.SetObject("log.thread.name", value); } }

        /// <summary>
        ///     Log broker disconnects. It might be useful to turn this off when interacting with 0.9 brokers with an aggressive `connection.max.idle.ms` value.
        ///
        ///     default: true
        ///     importance: low
        /// </summary>
        public bool? LogConnectionClose { get { return GetBool("log.connection.close"); } set { this.SetObject("log.connection.close", value); } }

        /// <summary>
        ///     Signal that librdkafka will use to quickly terminate on rd_kafka_destroy(). If this signal is not set then there will be a delay before rd_kafka_wait_destroyed() returns true as internal threads are timing out their system calls. If this signal is set however the delay will be minimal. The application should mask this signal as an internal signal handler is installed.
        ///
        ///     default: 0
        ///     importance: low
        /// </summary>
        public int? InternalTerminationSignal { get { return GetInt("internal.termination.signal"); } set { this.SetObject("internal.termination.signal", value); } }

        /// <summary>
        ///     Request broker's supported API versions to adjust functionality to available protocol features. If set to false, or the ApiVersionRequest fails, the fallback version `broker.version.fallback` will be used. **NOTE**: Depends on broker version >=0.10.0. If the request is not supported by (an older) broker the `broker.version.fallback` fallback is used.
        ///
        ///     default: true
        ///     importance: high
        /// </summary>
        public bool? ApiVersionRequest { get { return GetBool("api.version.request"); } set { this.SetObject("api.version.request", value); } }

        /// <summary>
        ///     Timeout for broker API version requests.
        ///
        ///     default: 10000
        ///     importance: low
        /// </summary>
        public int? ApiVersionRequestTimeoutMs { get { return GetInt("api.version.request.timeout.ms"); } set { this.SetObject("api.version.request.timeout.ms", value); } }

        /// <summary>
        ///     Dictates how long the `broker.version.fallback` fallback is used in the case the ApiVersionRequest fails. **NOTE**: The ApiVersionRequest is only issued when a new connection to the broker is made (such as after an upgrade).
        ///
        ///     default: 0
        ///     importance: medium
        /// </summary>
        public int? ApiVersionFallbackMs { get { return GetInt("api.version.fallback.ms"); } set { this.SetObject("api.version.fallback.ms", value); } }

        /// <summary>
        ///     Older broker versions (before 0.10.0) provide no way for a client to query for supported protocol features (ApiVersionRequest, see `api.version.request`) making it impossible for the client to know what features it may use. As a workaround a user may set this property to the expected broker version and the client will automatically adjust its feature set accordingly if the ApiVersionRequest fails (or is disabled). The fallback broker version will be used for `api.version.fallback.ms`. Valid values are: 0.9.0, 0.8.2, 0.8.1, 0.8.0. Any other value >= 0.10, such as 0.10.2.1, enables ApiVersionRequests.
        ///
        ///     default: 0.10.0
        ///     importance: medium
        /// </summary>
        public string BrokerVersionFallback { get { return Get("broker.version.fallback"); } set { this.SetObject("broker.version.fallback", value); } }

        /// <summary>
        ///     Protocol used to communicate with brokers.
        ///
        ///     default: plaintext
        ///     importance: high
        /// </summary>
        public SecurityProtocol? SecurityProtocol { get { return (SecurityProtocol?)GetEnum(typeof(SecurityProtocol), "security.protocol"); } set { this.SetObject("security.protocol", value); } }

        /// <summary>
        ///     A cipher suite is a named combination of authentication, encryption, MAC and key exchange algorithm used to negotiate the security settings for a network connection using TLS or SSL network protocol. See manual page for `ciphers(1)` and `SSL_CTX_set_cipher_list(3).
        ///
        ///     default: ''
        ///     importance: low
        /// </summary>
        public string SslCipherSuites { get { return Get("ssl.cipher.suites"); } set { this.SetObject("ssl.cipher.suites", value); } }

        /// <summary>
        ///     The supported-curves extension in the TLS ClientHello message specifies the curves (standard/named, or 'explicit' GF(2^k) or GF(p)) the client is willing to have the server use. See manual page for `SSL_CTX_set1_curves_list(3)`. OpenSSL >= 1.0.2 required.
        ///
        ///     default: ''
        ///     importance: low
        /// </summary>
        public string SslCurvesList { get { return Get("ssl.curves.list"); } set { this.SetObject("ssl.curves.list", value); } }

        /// <summary>
        ///     The client uses the TLS ClientHello signature_algorithms extension to indicate to the server which signature/hash algorithm pairs may be used in digital signatures. See manual page for `SSL_CTX_set1_sigalgs_list(3)`. OpenSSL >= 1.0.2 required.
        ///
        ///     default: ''
        ///     importance: low
        /// </summary>
        public string SslSigalgsList { get { return Get("ssl.sigalgs.list"); } set { this.SetObject("ssl.sigalgs.list", value); } }

        /// <summary>
        ///     Path to client's private key (PEM) used for authentication.
        ///
        ///     default: ''
        ///     importance: low
        /// </summary>
        public string SslKeyLocation { get { return Get("ssl.key.location"); } set { this.SetObject("ssl.key.location", value); } }

        /// <summary>
        ///     Private key passphrase
        ///
        ///     default: ''
        ///     importance: low
        /// </summary>
        public string SslKeyPassword { get { return Get("ssl.key.password"); } set { this.SetObject("ssl.key.password", value); } }

        /// <summary>
        ///     Path to client's public key (PEM) used for authentication.
        ///
        ///     default: ''
        ///     importance: low
        /// </summary>
        public string SslCertificateLocation { get { return Get("ssl.certificate.location"); } set { this.SetObject("ssl.certificate.location", value); } }

        /// <summary>
        ///     File or directory path to CA certificate(s) for verifying the broker's key.
        ///
        ///     default: ''
        ///     importance: medium
        /// </summary>
        public string SslCaLocation { get { return Get("ssl.ca.location"); } set { this.SetObject("ssl.ca.location", value); } }

        /// <summary>
        ///     Path to CRL for verifying broker's certificate validity.
        ///
        ///     default: ''
        ///     importance: low
        /// </summary>
        public string SslCrlLocation { get { return Get("ssl.crl.location"); } set { this.SetObject("ssl.crl.location", value); } }

        /// <summary>
        ///     Path to client's keystore (PKCS#12) used for authentication.
        ///
        ///     default: ''
        ///     importance: low
        /// </summary>
        public string SslKeystoreLocation { get { return Get("ssl.keystore.location"); } set { this.SetObject("ssl.keystore.location", value); } }

        /// <summary>
        ///     Client's keystore (PKCS#12) password.
        ///
        ///     default: ''
        ///     importance: low
        /// </summary>
        public string SslKeystorePassword { get { return Get("ssl.keystore.password"); } set { this.SetObject("ssl.keystore.password", value); } }

        /// <summary>
        ///     Kerberos principal name that Kafka runs as, not including /hostname@REALM
        ///
        ///     default: kafka
        ///     importance: low
        /// </summary>
        public string SaslKerberosServiceName { get { return Get("sasl.kerberos.service.name"); } set { this.SetObject("sasl.kerberos.service.name", value); } }

        /// <summary>
        ///     This client's Kerberos principal name. (Not supported on Windows, will use the logon user's principal).
        ///
        ///     default: kafkaclient
        ///     importance: low
        /// </summary>
        public string SaslKerberosPrincipal { get { return Get("sasl.kerberos.principal"); } set { this.SetObject("sasl.kerberos.principal", value); } }

        /// <summary>
        ///     Full kerberos kinit command string, %{config.prop.name} is replaced by corresponding config object value, %{broker.name} returns the broker's hostname.
        ///
        ///     default: kinit -S "%{sasl.kerberos.service.name}/%{broker.name}" -k -t "%{sasl.kerberos.keytab}" %{sasl.kerberos.principal}
        ///     importance: low
        /// </summary>
        public string SaslKerberosKinitCmd { get { return Get("sasl.kerberos.kinit.cmd"); } set { this.SetObject("sasl.kerberos.kinit.cmd", value); } }

        /// <summary>
        ///     Path to Kerberos keytab file. Uses system default if not set.**NOTE**: This is not automatically used but must be added to the template in sasl.kerberos.kinit.cmd as ` ... -t %{sasl.kerberos.keytab}`.
        ///
        ///     default: ''
        ///     importance: low
        /// </summary>
        public string SaslKerberosKeytab { get { return Get("sasl.kerberos.keytab"); } set { this.SetObject("sasl.kerberos.keytab", value); } }

        /// <summary>
        ///     Minimum time in milliseconds between key refresh attempts.
        ///
        ///     default: 60000
        ///     importance: low
        /// </summary>
        public int? SaslKerberosMinTimeBeforeRelogin { get { return GetInt("sasl.kerberos.min.time.before.relogin"); } set { this.SetObject("sasl.kerberos.min.time.before.relogin", value); } }

        /// <summary>
        ///     SASL username for use with the PLAIN and SASL-SCRAM-.. mechanisms
        ///
        ///     default: ''
        ///     importance: high
        /// </summary>
        public string SaslUsername { get { return Get("sasl.username"); } set { this.SetObject("sasl.username", value); } }

        /// <summary>
        ///     SASL password for use with the PLAIN and SASL-SCRAM-.. mechanism
        ///
        ///     default: ''
        ///     importance: high
        /// </summary>
        public string SaslPassword { get { return Get("sasl.password"); } set { this.SetObject("sasl.password", value); } }

        /// <summary>
        ///     List of plugin libraries to load (; separated). The library search path is platform dependent (see dlopen(3) for Unix and LoadLibrary() for Windows). If no filename extension is specified the platform-specific extension (such as .dll or .so) will be appended automatically.
        ///
        ///     default: ''
        ///     importance: low
        /// </summary>
        public string PluginLibraryPaths { get { return Get("plugin.library.paths"); } set { this.SetObject("plugin.library.paths", value); } }

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
        ///     importance: low
        /// </summary>
        public bool? EnableBackgroundPoll { get { return GetBool("dotnet.producer.enable.background.poll"); } set { this.SetObject("dotnet.producer.enable.background.poll", value); } }

        /// <summary>
        ///     Specifies whether to enable notification of delivery reports. Typically
        ///     you should set this parameter to true. Set it to false for "fire and
        ///     forget" semantics and a small boost in performance.
        /// 
        ///     default: true
        ///     importance: low
        /// </summary>
        public bool? EnableDeliveryReports { get { return GetBool("dotnet.producer.enable.delivery.reports"); } set { this.SetObject("dotnet.producer.enable.delivery.reports", value); } }

        /// <summary>
        ///     A comma separated list of fields that may be optionally set in delivery
        ///     reports. Disabling delivery report fields that you do not require will
        ///     improve maximum throughput and reduce memory usage. Allowed values:
        ///     key, value, timestamp, headers, all, none.
        /// 
        ///     default: all
        ///     importance: low
        /// </summary>
        public string DeliveryReportFields { get { return Get("dotnet.producer.delivery.report.fields"); } set { this.SetObject("dotnet.producer.delivery.report.fields", value.ToString()); } }

        /// <summary>
        ///     When set to `true`, the producer will ensure that messages are successfully produced exactly once and in the original produce order. The following configuration properties are adjusted automatically (if not modified by the user) when idempotence is enabled: `max.in.flight.requests.per.connection=5` (must be less than or equal to 5), `retries=INT32_MAX` (must be greater than 0), `acks=all`, `queuing.strategy=fifo`. Producer instantation will fail if user-supplied configuration is incompatible.
        ///
        ///     default: false
        ///     importance: high
        /// </summary>
        public bool? EnableIdempotence { get { return GetBool("enable.idempotence"); } set { this.SetObject("enable.idempotence", value); } }

        /// <summary>
        ///     When set to `true`, any error that could result in a gap in the produced message series when a batch of messages fails, will raise a fatal error (ERR__GAPLESS_GUARANTEE) and stop the producer. Requires `enable.idempotence=true`.
        ///
        ///     default: true
        ///     importance: high
        /// </summary>
        public bool? EnableGaplessGuarantee { get { return GetBool("enable.gapless.guarantee"); } set { this.SetObject("enable.gapless.guarantee", value); } }

        /// <summary>
        ///     Maximum number of messages allowed on the producer queue. This queue is shared by all topics and partitions.
        ///
        ///     default: 100000
        ///     importance: high
        /// </summary>
        public int? QueueBufferingMaxMessages { get { return GetInt("queue.buffering.max.messages"); } set { this.SetObject("queue.buffering.max.messages", value); } }

        /// <summary>
        ///     Maximum total message size sum allowed on the producer queue. This queue is shared by all topics and partitions. This property has higher priority than queue.buffering.max.messages.
        ///
        ///     default: 1048576
        ///     importance: high
        /// </summary>
        public int? QueueBufferingMaxKbytes { get { return GetInt("queue.buffering.max.kbytes"); } set { this.SetObject("queue.buffering.max.kbytes", value); } }

        /// <summary>
        ///     Delay in milliseconds to wait for messages in the producer queue to accumulate before constructing message batches (MessageSets) to transmit to brokers. A higher value allows larger and more effective (less overhead, improved compression) batches of messages to accumulate at the expense of increased message delivery latency.
        ///
        ///     default: 0
        ///     importance: high
        /// </summary>
        public int? LingerMs { get { return GetInt("linger.ms"); } set { this.SetObject("linger.ms", value); } }

        /// <summary>
        ///     How many times to retry sending a failing Message. **Note:** retrying may cause reordering unless `enable.idempotence` is set to true.
        ///
        ///     default: 2
        ///     importance: high
        /// </summary>
        public int? MessageSendMaxRetries { get { return GetInt("message.send.max.retries"); } set { this.SetObject("message.send.max.retries", value); } }

        /// <summary>
        ///     The backoff time in milliseconds before retrying a protocol request.
        ///
        ///     default: 100
        ///     importance: medium
        /// </summary>
        public int? RetryBackoffMs { get { return GetInt("retry.backoff.ms"); } set { this.SetObject("retry.backoff.ms", value); } }

        /// <summary>
        ///     The threshold of outstanding not yet transmitted broker requests needed to backpressure the producer's message accumulator. If the number of not yet transmitted requests equals or exceeds this number, produce request creation that would have otherwise been triggered (for example, in accordance with linger.ms) will be delayed. A lower number yields larger and more effective batches. A higher value can improve latency when using compression on slow machines.
        ///
        ///     default: 1
        ///     importance: low
        /// </summary>
        public int? QueueBufferingBackpressureThreshold { get { return GetInt("queue.buffering.backpressure.threshold"); } set { this.SetObject("queue.buffering.backpressure.threshold", value); } }

        /// <summary>
        ///     Maximum number of messages batched in one MessageSet. The total MessageSet size is also limited by message.max.bytes.
        ///
        ///     default: 10000
        ///     importance: medium
        /// </summary>
        public int? BatchNumMessages { get { return GetInt("batch.num.messages"); } set { this.SetObject("batch.num.messages", value); } }

        /// <summary>
        ///     The ack timeout of the producer request in milliseconds. This value is only enforced by the broker and relies on `request.required.acks` being != 0.
        ///
        ///     default: 5000
        ///     importance: medium
        /// </summary>
        public int? RequestTimeoutMs { get { return GetInt("request.timeout.ms"); } set { this.SetObject("request.timeout.ms", value); } }

        /// <summary>
        ///     Local message timeout. This value is only enforced locally and limits the time a produced message waits for successful delivery. A time of 0 is infinite. This is the maximum time librdkafka may use to deliver a message (including retries). Delivery error occurs when either the retry count or the message timeout are exceeded.
        ///
        ///     default: 300000
        ///     importance: high
        /// </summary>
        public int? MessageTimeoutMs { get { return GetInt("message.timeout.ms"); } set { this.SetObject("message.timeout.ms", value); } }

        /// <summary>
        ///     Partitioner: `random` - random distribution, `consistent` - CRC32 hash of key (Empty and NULL keys are mapped to single partition), `consistent_random` - CRC32 hash of key (Empty and NULL keys are randomly partitioned), `murmur2` - Java Producer compatible Murmur2 hash of key (NULL keys are mapped to single partition), `murmur2_random` - Java Producer compatible Murmur2 hash of key (NULL keys are randomly partitioned. This is functionally equivalent to the default partitioner in the Java Producer.).
        ///
        ///     default: consistent_random
        ///     importance: high
        /// </summary>
        public Partitioner? Partitioner { get { return (Partitioner?)GetEnum(typeof(Partitioner), "partitioner"); } set { this.SetObject("partitioner", value); } }

        /// <summary>
        ///     Compression level parameter for algorithm selected by configuration property `compression.codec`. Higher values will result in better compression at the cost of more CPU usage. Usable range is algorithm-dependent: [0-9] for gzip; [0-12] for lz4; only 0 for snappy; -1 = codec-dependent default compression level.
        ///
        ///     default: -1
        ///     importance: medium
        /// </summary>
        public int? CompressionLevel { get { return GetInt("compression.level"); } set { this.SetObject("compression.level", value); } }

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
        ///     <see cref="Confluent.Kafka.Consumer.Consume(System.TimeSpan)" />
        ///     method. Disabling fields that you do not require will improve 
        ///     throughput and reduce memory consumption. Allowed values:
        ///     headers, timestamp, topic, all, none
        /// 
        ///     default: all
        ///     importance: low
        /// </summary>
        public string ConsumeResultFields { set { this.SetObject("dotnet.consumer.consume.result.fields", value); } }

        /// <summary>
        ///     Client group id string. All clients sharing the same group.id belong to the same group.
        ///
        ///     default: ''
        ///     importance: high
        /// </summary>
        public string GroupId { get { return Get("group.id"); } set { this.SetObject("group.id", value); } }

        /// <summary>
        ///     Name of partition assignment strategy to use when elected group leader assigns partitions to group members.
        ///
        ///     default: range,roundrobin
        ///     importance: medium
        /// </summary>
        public PartitionAssignmentStrategy? PartitionAssignmentStrategy { get { return (PartitionAssignmentStrategy?)GetEnum(typeof(PartitionAssignmentStrategy), "partition.assignment.strategy"); } set { this.SetObject("partition.assignment.strategy", value); } }

        /// <summary>
        ///     Client group session and failure detection timeout. The consumer sends periodic heartbeats (heartbeat.interval.ms) to indicate its liveness to the broker. If no hearts are received by the broker for a group member within the session timeout, the broker will remove the consumer from the group and trigger a rebalance. The allowed range is configured with the **broker** configuration properties `group.min.session.timeout.ms` and `group.max.session.timeout.ms`. Also see `max.poll.interval.ms`.
        ///
        ///     default: 10000
        ///     importance: high
        /// </summary>
        public int? SessionTimeoutMs { get { return GetInt("session.timeout.ms"); } set { this.SetObject("session.timeout.ms", value); } }

        /// <summary>
        ///     Group session keepalive heartbeat interval.
        ///
        ///     default: 3000
        ///     importance: low
        /// </summary>
        public int? HeartbeatIntervalMs { get { return GetInt("heartbeat.interval.ms"); } set { this.SetObject("heartbeat.interval.ms", value); } }

        /// <summary>
        ///     Group protocol type
        ///
        ///     default: consumer
        ///     importance: low
        /// </summary>
        public string GroupProtocolType { get { return Get("group.protocol.type"); } set { this.SetObject("group.protocol.type", value); } }

        /// <summary>
        ///     How often to query for the current client group coordinator. If the currently assigned coordinator is down the configured query interval will be divided by ten to more quickly recover in case of coordinator reassignment.
        ///
        ///     default: 600000
        ///     importance: low
        /// </summary>
        public int? CoordinatorQueryIntervalMs { get { return GetInt("coordinator.query.interval.ms"); } set { this.SetObject("coordinator.query.interval.ms", value); } }

        /// <summary>
        ///     Maximum allowed time between calls to consume messages (e.g., rd_kafka_consumer_poll()) for high-level consumers. If this interval is exceeded the consumer is considered failed and the group will rebalance in order to reassign the partitions to another consumer group member. Warning: Offset commits may be not possible at this point. Note: It is recommended to set `enable.auto.offset.store=false` for long-time processing applications and then explicitly store offsets (using offsets_store()) *after* message processing, to make sure offsets are not auto-committed prior to processing has finished. The interval is checked two times per second. See KIP-62 for more information.
        ///
        ///     default: 300000
        ///     importance: high
        /// </summary>
        public int? MaxPollIntervalMs { get { return GetInt("max.poll.interval.ms"); } set { this.SetObject("max.poll.interval.ms", value); } }

        /// <summary>
        ///     Automatically and periodically commit offsets in the background. Note: setting this to false does not prevent the consumer from fetching previously committed start offsets. To circumvent this behaviour set specific start offsets per partition in the call to assign().
        ///
        ///     default: true
        ///     importance: high
        /// </summary>
        public bool? EnableAutoCommit { get { return GetBool("enable.auto.commit"); } set { this.SetObject("enable.auto.commit", value); } }

        /// <summary>
        ///     The frequency in milliseconds that the consumer offsets are committed (written) to offset storage. (0 = disable). This setting is used by the high-level consumer.
        ///
        ///     default: 5000
        ///     importance: medium
        /// </summary>
        public int? AutoCommitIntervalMs { get { return GetInt("auto.commit.interval.ms"); } set { this.SetObject("auto.commit.interval.ms", value); } }

        /// <summary>
        ///     Automatically store offset of last message provided to application. The offset store is an in-memory store of the next offset to (auto-)commit for each partition.
        ///
        ///     default: true
        ///     importance: high
        /// </summary>
        public bool? EnableAutoOffsetStore { get { return GetBool("enable.auto.offset.store"); } set { this.SetObject("enable.auto.offset.store", value); } }

        /// <summary>
        ///     Minimum number of messages per topic+partition librdkafka tries to maintain in the local consumer queue.
        ///
        ///     default: 100000
        ///     importance: medium
        /// </summary>
        public int? QueuedMinMessages { get { return GetInt("queued.min.messages"); } set { this.SetObject("queued.min.messages", value); } }

        /// <summary>
        ///     Maximum number of kilobytes per topic+partition in the local consumer queue. This value may be overshot by fetch.message.max.bytes. This property has higher priority than queued.min.messages.
        ///
        ///     default: 1048576
        ///     importance: medium
        /// </summary>
        public int? QueuedMaxMessagesKbytes { get { return GetInt("queued.max.messages.kbytes"); } set { this.SetObject("queued.max.messages.kbytes", value); } }

        /// <summary>
        ///     Maximum time the broker may wait to fill the response with fetch.min.bytes.
        ///
        ///     default: 100
        ///     importance: low
        /// </summary>
        public int? FetchWaitMaxMs { get { return GetInt("fetch.wait.max.ms"); } set { this.SetObject("fetch.wait.max.ms", value); } }

        /// <summary>
        ///     Initial maximum number of bytes per topic+partition to request when fetching messages from the broker. If the client encounters a message larger than this value it will gradually try to increase it until the entire message can be fetched.
        ///
        ///     default: 1048576
        ///     importance: medium
        /// </summary>
        public int? MaxPartitionFetchBytes { get { return GetInt("max.partition.fetch.bytes"); } set { this.SetObject("max.partition.fetch.bytes", value); } }

        /// <summary>
        ///     Maximum amount of data the broker shall return for a Fetch request. Messages are fetched in batches by the consumer and if the first message batch in the first non-empty partition of the Fetch request is larger than this value, then the message batch will still be returned to ensure the consumer can make progress. The maximum message batch size accepted by the broker is defined via `message.max.bytes` (broker config) or `max.message.bytes` (broker topic config). `fetch.max.bytes` is automatically adjusted upwards to be at least `message.max.bytes` (consumer config).
        ///
        ///     default: 52428800
        ///     importance: medium
        /// </summary>
        public int? FetchMaxBytes { get { return GetInt("fetch.max.bytes"); } set { this.SetObject("fetch.max.bytes", value); } }

        /// <summary>
        ///     Minimum number of bytes the broker responds with. If fetch.wait.max.ms expires the accumulated data will be sent to the client regardless of this setting.
        ///
        ///     default: 1
        ///     importance: low
        /// </summary>
        public int? FetchMinBytes { get { return GetInt("fetch.min.bytes"); } set { this.SetObject("fetch.min.bytes", value); } }

        /// <summary>
        ///     How long to postpone the next fetch request for a topic+partition in case of a fetch error.
        ///
        ///     default: 500
        ///     importance: medium
        /// </summary>
        public int? FetchErrorBackoffMs { get { return GetInt("fetch.error.backoff.ms"); } set { this.SetObject("fetch.error.backoff.ms", value); } }

        /// <summary>
        ///     Emit RD_KAFKA_RESP_ERR__PARTITION_EOF event whenever the consumer reaches the end of a partition.
        ///
        ///     default: false
        ///     importance: low
        /// </summary>
        public bool? EnablePartitionEof { get { return GetBool("enable.partition.eof"); } set { this.SetObject("enable.partition.eof", value); } }

        /// <summary>
        ///     Verify CRC32 of consumed messages, ensuring no on-the-wire or on-disk corruption to the messages occurred. This check comes at slightly increased CPU usage.
        ///
        ///     default: false
        ///     importance: medium
        /// </summary>
        public bool? CheckCrcs { get { return GetBool("check.crcs"); } set { this.SetObject("check.crcs", value); } }

        /// <summary>
        ///     Action to take when there is no initial offset in offset store or the desired offset is out of range: 'smallest','earliest' - automatically reset the offset to the smallest offset, 'largest','latest' - automatically reset the offset to the largest offset, 'error' - trigger an error which is retrieved by consuming messages and checking 'message->err'.
        ///
        ///     default: largest
        ///     importance: high
        /// </summary>
        public AutoOffsetReset? AutoOffsetReset { get { return (AutoOffsetReset?)GetEnum(typeof(AutoOffsetReset), "auto.offset.reset"); } set { this.SetObject("auto.offset.reset", value); } }

    }

}
