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
using System.Runtime.Serialization;


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
    [DataContract]
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
        [DataMember(Name="client.id")]
        public string ClientId { get { return Get("client.id"); } set { this.SetObject("client.id", value); } }

        /// <summary>
        ///     Initial list of brokers as a CSV list of broker host or host:port. The application may also use `rd_kafka_brokers_add()` to add brokers during runtime.
        /// </summary>
        [DataMember(Name="bootstrap.servers")]
        public string BootstrapServers { get { return Get("bootstrap.servers"); } set { this.SetObject("bootstrap.servers", value); } }

        /// <summary>
        ///     Maximum Kafka protocol request message size.
        /// </summary>
        [DataMember(Name="message.max.bytes")]
        public int? MessageMaxBytes { get { return GetInt("message.max.bytes"); } set { this.SetObject("message.max.bytes", value); } }

        /// <summary>
        ///     Maximum size for message to be copied to buffer. Messages larger than this will be passed by reference (zero-copy) at the expense of larger iovecs.
        /// </summary>
        [DataMember(Name="message.copy.max.bytes")]
        public int? MessageCopyMaxBytes { get { return GetInt("message.copy.max.bytes"); } set { this.SetObject("message.copy.max.bytes", value); } }

        /// <summary>
        ///     Maximum Kafka protocol response message size. This serves as a safety precaution to avoid memory exhaustion in case of protocol hickups. This value is automatically adjusted upwards to be at least `fetch.max.bytes` + 512 to allow for protocol overhead.
        /// </summary>
        [DataMember(Name="receive.message.max.bytes")]
        public int? ReceiveMessageMaxBytes { get { return GetInt("receive.message.max.bytes"); } set { this.SetObject("receive.message.max.bytes", value); } }

        /// <summary>
        ///     Maximum number of in-flight requests per broker connection. This is a generic property applied to all broker communication, however it is primarily relevant to produce requests. In particular, note that other mechanisms limit the number of outstanding consumer fetch request per broker to one.
        /// </summary>
        [DataMember(Name="max.in.flight")]
        public int? MaxInFlight { get { return GetInt("max.in.flight"); } set { this.SetObject("max.in.flight", value); } }

        /// <summary>
        ///     Non-topic request timeout in milliseconds. This is for metadata requests, etc.
        /// </summary>
        [DataMember(Name="metadata.request.timeout.ms")]
        public int? MetadataRequestTimeoutMs { get { return GetInt("metadata.request.timeout.ms"); } set { this.SetObject("metadata.request.timeout.ms", value); } }

        /// <summary>
        ///     Topic metadata refresh interval in milliseconds. The metadata is automatically refreshed on error and connect. Use -1 to disable the intervalled refresh.
        /// </summary>
        [DataMember(Name="topic.metadata.refresh.interval.ms")]
        public int? TopicMetadataRefreshIntervalMs { get { return GetInt("topic.metadata.refresh.interval.ms"); } set { this.SetObject("topic.metadata.refresh.interval.ms", value); } }

        /// <summary>
        ///     Metadata cache max age. Defaults to topic.metadata.refresh.interval.ms * 3
        /// </summary>
        [DataMember(Name="metadata.max.age.ms")]
        public int? MetadataMaxAgeMs { get { return GetInt("metadata.max.age.ms"); } set { this.SetObject("metadata.max.age.ms", value); } }

        /// <summary>
        ///     When a topic loses its leader a new metadata request will be enqueued with this initial interval, exponentially increasing until the topic metadata has been refreshed. This is used to recover quickly from transitioning leader brokers.
        /// </summary>
        [DataMember(Name="topic.metadata.refresh.fast.interval.ms")]
        public int? TopicMetadataRefreshFastIntervalMs { get { return GetInt("topic.metadata.refresh.fast.interval.ms"); } set { this.SetObject("topic.metadata.refresh.fast.interval.ms", value); } }

        /// <summary>
        ///     Sparse metadata requests (consumes less network bandwidth)
        /// </summary>
        [DataMember(Name="topic.metadata.refresh.sparse")]
        public bool? TopicMetadataRefreshSparse { get { return GetBool("topic.metadata.refresh.sparse"); } set { this.SetObject("topic.metadata.refresh.sparse", value); } }

        /// <summary>
        ///     Topic blacklist, a comma-separated list of regular expressions for matching topic names that should be ignored in broker metadata information as if the topics did not exist.
        /// </summary>
        [DataMember(Name="topic.blacklist")]
        public string TopicBlacklist { get { return Get("topic.blacklist"); } set { this.SetObject("topic.blacklist", value); } }

        /// <summary>
        ///     A comma-separated list of debug contexts to enable. Detailed Producer debugging: broker,topic,msg. Consumer: consumer,cgrp,topic,fetch
        /// </summary>
        [DataMember(Name="debug")]
        public string Debug { get { return Get("debug"); } set { this.SetObject("debug", value); } }

        /// <summary>
        ///     Default timeout for network requests. Producer: ProduceRequests will use the lesser value of `socket.timeout.ms` and remaining `message.timeout.ms` for the first message in the batch. Consumer: FetchRequests will use `fetch.wait.max.ms` + `socket.timeout.ms`. Admin: Admin requests will use `socket.timeout.ms` or explicitly set `rd_kafka_AdminOptions_set_operation_timeout()` value.
        /// </summary>
        [DataMember(Name="socket.timeout.ms")]
        public int? SocketTimeoutMs { get { return GetInt("socket.timeout.ms"); } set { this.SetObject("socket.timeout.ms", value); } }

        /// <summary>
        ///     Maximum time a broker socket operation may block. A lower value improves responsiveness at the expense of slightly higher CPU usage. **Deprecated**
        /// </summary>
        [DataMember(Name="socket.blocking.max.ms")]
        public int? SocketBlockingMaxMs { get { return GetInt("socket.blocking.max.ms"); } set { this.SetObject("socket.blocking.max.ms", value); } }

        /// <summary>
        ///     Broker socket send buffer size. System default is used if 0.
        /// </summary>
        [DataMember(Name="socket.send.buffer.bytes")]
        public int? SocketSendBufferBytes { get { return GetInt("socket.send.buffer.bytes"); } set { this.SetObject("socket.send.buffer.bytes", value); } }

        /// <summary>
        ///     Broker socket receive buffer size. System default is used if 0.
        /// </summary>
        [DataMember(Name="socket.receive.buffer.bytes")]
        public int? SocketReceiveBufferBytes { get { return GetInt("socket.receive.buffer.bytes"); } set { this.SetObject("socket.receive.buffer.bytes", value); } }

        /// <summary>
        ///     Enable TCP keep-alives (SO_KEEPALIVE) on broker sockets
        /// </summary>
        [DataMember(Name="socket.keepalive.enable")]
        public bool? SocketKeepaliveEnable { get { return GetBool("socket.keepalive.enable"); } set { this.SetObject("socket.keepalive.enable", value); } }

        /// <summary>
        ///     Disable the Nagle algorithm (TCP_NODELAY) on broker sockets.
        /// </summary>
        [DataMember(Name="socket.nagle.disable")]
        public bool? SocketNagleDisable { get { return GetBool("socket.nagle.disable"); } set { this.SetObject("socket.nagle.disable", value); } }

        /// <summary>
        ///     Disconnect from broker when this number of send failures (e.g., timed out requests) is reached. Disable with 0. WARNING: It is highly recommended to leave this setting at its default value of 1 to avoid the client and broker to become desynchronized in case of request timeouts. NOTE: The connection is automatically re-established.
        /// </summary>
        [DataMember(Name="socket.max.fails")]
        public int? SocketMaxFails { get { return GetInt("socket.max.fails"); } set { this.SetObject("socket.max.fails", value); } }

        /// <summary>
        ///     How long to cache the broker address resolving results (milliseconds).
        /// </summary>
        [DataMember(Name="broker.address.ttl")]
        public int? BrokerAddressTtl { get { return GetInt("broker.address.ttl"); } set { this.SetObject("broker.address.ttl", value); } }

        /// <summary>
        ///     Allowed broker IP address families: any, v4, v6
        /// </summary>
        [DataMember(Name="broker.address.family")]
        public BrokerAddressFamilyType? BrokerAddressFamily { get { return (BrokerAddressFamilyType?)GetEnum(typeof(BrokerAddressFamilyType), "broker.address.family"); } set { this.SetObject("broker.address.family", value); } }

        /// <summary>
        ///     Throttle broker reconnection attempts by this value +-50%.
        /// </summary>
        [DataMember(Name="reconnect.backoff.jitter.ms")]
        public int? ReconnectBackoffJitterMs { get { return GetInt("reconnect.backoff.jitter.ms"); } set { this.SetObject("reconnect.backoff.jitter.ms", value); } }

        /// <summary>
        ///     librdkafka statistics emit interval. The application also needs to register a stats callback using `rd_kafka_conf_set_stats_cb()`. The granularity is 1000ms. A value of 0 disables statistics.
        /// </summary>
        [DataMember(Name="statistics.interval.ms")]
        public int? StatisticsIntervalMs { get { return GetInt("statistics.interval.ms"); } set { this.SetObject("statistics.interval.ms", value); } }

        /// <summary>
        ///     Logging level (syslog(3) levels)
        /// </summary>
        [DataMember(Name="log_level")]
        public int? Log_level { get { return GetInt("log_level"); } set { this.SetObject("log_level", value); } }

        /// <summary>
        ///     Disable spontaneous log_cb from internal librdkafka threads, instead enqueue log messages on queue set with `rd_kafka_set_log_queue()` and serve log callbacks or events through the standard poll APIs. **NOTE**: Log messages will linger in a temporary queue until the log queue has been set.
        /// </summary>
        [DataMember(Name="log.queue")]
        public bool? LogQueue { get { return GetBool("log.queue"); } set { this.SetObject("log.queue", value); } }

        /// <summary>
        ///     Print internal thread name in log messages (useful for debugging librdkafka internals)
        /// </summary>
        [DataMember(Name="log.thread.name")]
        public bool? LogThreadName { get { return GetBool("log.thread.name"); } set { this.SetObject("log.thread.name", value); } }

        /// <summary>
        ///     Log broker disconnects. It might be useful to turn this off when interacting with 0.9 brokers with an aggressive `connection.max.idle.ms` value.
        /// </summary>
        [DataMember(Name="log.connection.close")]
        public bool? LogConnectionClose { get { return GetBool("log.connection.close"); } set { this.SetObject("log.connection.close", value); } }

        /// <summary>
        ///     Signal that librdkafka will use to quickly terminate on rd_kafka_destroy(). If this signal is not set then there will be a delay before rd_kafka_wait_destroyed() returns true as internal threads are timing out their system calls. If this signal is set however the delay will be minimal. The application should mask this signal as an internal signal handler is installed.
        /// </summary>
        [DataMember(Name="internal.termination.signal")]
        public int? InternalTerminationSignal { get { return GetInt("internal.termination.signal"); } set { this.SetObject("internal.termination.signal", value); } }

        /// <summary>
        ///     Request broker's supported API versions to adjust functionality to available protocol features. If set to false, or the ApiVersionRequest fails, the fallback version `broker.version.fallback` will be used. **NOTE**: Depends on broker version >=0.10.0. If the request is not supported by (an older) broker the `broker.version.fallback` fallback is used.
        /// </summary>
        [DataMember(Name="api.version.request")]
        public bool? ApiVersionRequest { get { return GetBool("api.version.request"); } set { this.SetObject("api.version.request", value); } }

        /// <summary>
        ///     Timeout for broker API version requests.
        /// </summary>
        [DataMember(Name="api.version.request.timeout.ms")]
        public int? ApiVersionRequestTimeoutMs { get { return GetInt("api.version.request.timeout.ms"); } set { this.SetObject("api.version.request.timeout.ms", value); } }

        /// <summary>
        ///     Dictates how long the `broker.version.fallback` fallback is used in the case the ApiVersionRequest fails. **NOTE**: The ApiVersionRequest is only issued when a new connection to the broker is made (such as after an upgrade).
        /// </summary>
        [DataMember(Name="api.version.fallback.ms")]
        public int? ApiVersionFallbackMs { get { return GetInt("api.version.fallback.ms"); } set { this.SetObject("api.version.fallback.ms", value); } }

        /// <summary>
        ///     Older broker versions (before 0.10.0) provide no way for a client to query for supported protocol features (ApiVersionRequest, see `api.version.request`) making it impossible for the client to know what features it may use. As a workaround a user may set this property to the expected broker version and the client will automatically adjust its feature set accordingly if the ApiVersionRequest fails (or is disabled). The fallback broker version will be used for `api.version.fallback.ms`. Valid values are: 0.9.0, 0.8.2, 0.8.1, 0.8.0. Any other value, such as 0.10.2.1, enables ApiVersionRequests.
        /// </summary>
        [DataMember(Name="broker.version.fallback")]
        public string BrokerVersionFallback { get { return Get("broker.version.fallback"); } set { this.SetObject("broker.version.fallback", value); } }

        /// <summary>
        ///     Protocol used to communicate with brokers.
        /// </summary>
        [DataMember(Name="security.protocol")]
        public SecurityProtocolType? SecurityProtocol { get { return (SecurityProtocolType?)GetEnum(typeof(SecurityProtocolType), "security.protocol"); } set { this.SetObject("security.protocol", value); } }

        /// <summary>
        ///     A cipher suite is a named combination of authentication, encryption, MAC and key exchange algorithm used to negotiate the security settings for a network connection using TLS or SSL network protocol. See manual page for `ciphers(1)` and `SSL_CTX_set_cipher_list(3).
        /// </summary>
        [DataMember(Name="ssl.cipher.suites")]
        public string SslCipherSuites { get { return Get("ssl.cipher.suites"); } set { this.SetObject("ssl.cipher.suites", value); } }

        /// <summary>
        ///     The supported-curves extension in the TLS ClientHello message specifies the curves (standard/named, or 'explicit' GF(2^k) or GF(p)) the client is willing to have the server use. See manual page for `SSL_CTX_set1_curves_list(3)`. OpenSSL >= 1.0.2 required.
        /// </summary>
        [DataMember(Name="ssl.curves.list")]
        public string SslCurvesList { get { return Get("ssl.curves.list"); } set { this.SetObject("ssl.curves.list", value); } }

        /// <summary>
        ///     The client uses the TLS ClientHello signature_algorithms extension to indicate to the server which signature/hash algorithm pairs may be used in digital signatures. See manual page for `SSL_CTX_set1_sigalgs_list(3)`. OpenSSL >= 1.0.2 required.
        /// </summary>
        [DataMember(Name="ssl.sigalgs.list")]
        public string SslSigalgsList { get { return Get("ssl.sigalgs.list"); } set { this.SetObject("ssl.sigalgs.list", value); } }

        /// <summary>
        ///     Path to client's private key (PEM) used for authentication.
        /// </summary>
        [DataMember(Name="ssl.key.location")]
        public string SslKeyLocation { get { return Get("ssl.key.location"); } set { this.SetObject("ssl.key.location", value); } }

        /// <summary>
        ///     Private key passphrase
        /// </summary>
        [DataMember(Name="ssl.key.password")]
        public string SslKeyPassword { get { return Get("ssl.key.password"); } set { this.SetObject("ssl.key.password", value); } }

        /// <summary>
        ///     Path to client's public key (PEM) used for authentication.
        /// </summary>
        [DataMember(Name="ssl.certificate.location")]
        public string SslCertificateLocation { get { return Get("ssl.certificate.location"); } set { this.SetObject("ssl.certificate.location", value); } }

        /// <summary>
        ///     File or directory path to CA certificate(s) for verifying the broker's key.
        /// </summary>
        [DataMember(Name="ssl.ca.location")]
        public string SslCaLocation { get { return Get("ssl.ca.location"); } set { this.SetObject("ssl.ca.location", value); } }

        /// <summary>
        ///     Path to CRL for verifying broker's certificate validity.
        /// </summary>
        [DataMember(Name="ssl.crl.location")]
        public string SslCrlLocation { get { return Get("ssl.crl.location"); } set { this.SetObject("ssl.crl.location", value); } }

        /// <summary>
        ///     Path to client's keystore (PKCS#12) used for authentication.
        /// </summary>
        [DataMember(Name="ssl.keystore.location")]
        public string SslKeystoreLocation { get { return Get("ssl.keystore.location"); } set { this.SetObject("ssl.keystore.location", value); } }

        /// <summary>
        ///     Client's keystore (PKCS#12) password.
        /// </summary>
        [DataMember(Name="ssl.keystore.password")]
        public string SslKeystorePassword { get { return Get("ssl.keystore.password"); } set { this.SetObject("ssl.keystore.password", value); } }

        /// <summary>
        ///     SASL mechanism to use for authentication. Supported: GSSAPI, PLAIN, SCRAM-SHA-256, SCRAM-SHA-512. **NOTE**: Despite the name only one mechanism must be configured.
        /// </summary>
        [DataMember(Name="sasl.mechanism")]
        public string SaslMechanism { get { return Get("sasl.mechanism"); } set { this.SetObject("sasl.mechanism", value); } }

        /// <summary>
        ///     Kerberos principal name that Kafka runs as, not including /hostname@REALM
        /// </summary>
        [DataMember(Name="sasl.kerberos.service.name")]
        public string SaslKerberosServiceName { get { return Get("sasl.kerberos.service.name"); } set { this.SetObject("sasl.kerberos.service.name", value); } }

        /// <summary>
        ///     This client's Kerberos principal name. (Not supported on Windows, will use the logon user's principal).
        /// </summary>
        [DataMember(Name="sasl.kerberos.principal")]
        public string SaslKerberosPrincipal { get { return Get("sasl.kerberos.principal"); } set { this.SetObject("sasl.kerberos.principal", value); } }

        /// <summary>
        ///     Full kerberos kinit command string, %{config.prop.name} is replaced by corresponding config object value, %{broker.name} returns the broker's hostname.
        /// </summary>
        [DataMember(Name="sasl.kerberos.kinit.cmd")]
        public string SaslKerberosKinitCmd { get { return Get("sasl.kerberos.kinit.cmd"); } set { this.SetObject("sasl.kerberos.kinit.cmd", value); } }

        /// <summary>
        ///     Path to Kerberos keytab file. Uses system default if not set.**NOTE**: This is not automatically used but must be added to the template in sasl.kerberos.kinit.cmd as ` ... -t %{sasl.kerberos.keytab}`.
        /// </summary>
        [DataMember(Name="sasl.kerberos.keytab")]
        public string SaslKerberosKeytab { get { return Get("sasl.kerberos.keytab"); } set { this.SetObject("sasl.kerberos.keytab", value); } }

        /// <summary>
        ///     Minimum time in milliseconds between key refresh attempts.
        /// </summary>
        [DataMember(Name="sasl.kerberos.min.time.before.relogin")]
        public int? SaslKerberosMinTimeBeforeRelogin { get { return GetInt("sasl.kerberos.min.time.before.relogin"); } set { this.SetObject("sasl.kerberos.min.time.before.relogin", value); } }

        /// <summary>
        ///     SASL username for use with the PLAIN and SASL-SCRAM-.. mechanisms
        /// </summary>
        [DataMember(Name="sasl.username")]
        public string SaslUsername { get { return Get("sasl.username"); } set { this.SetObject("sasl.username", value); } }

        /// <summary>
        ///     SASL password for use with the PLAIN and SASL-SCRAM-.. mechanism
        /// </summary>
        [DataMember(Name="sasl.password")]
        public string SaslPassword { get { return Get("sasl.password"); } set { this.SetObject("sasl.password", value); } }

        /// <summary>
        ///     List of plugin libraries to load (; separated). The library search path is platform dependent (see dlopen(3) for Unix and LoadLibrary() for Windows). If no filename extension is specified the platform-specific extension (such as .dll or .so) will be appended automatically.
        /// </summary>
        [DataMember(Name="plugin.library.paths")]
        public string PluginLibraryPaths { get { return Get("plugin.library.paths"); } set { this.SetObject("plugin.library.paths", value); } }

        /// <summary>
        ///     Client group id string. All clients sharing the same group.id belong to the same group.
        /// </summary>
        [DataMember(Name="group.id")]
        public string GroupId { get { return Get("group.id"); } set { this.SetObject("group.id", value); } }

        /// <summary>
        ///     Name of partition assignment strategy to use when elected group leader assigns partitions to group members.
        /// </summary>
        [DataMember(Name="partition.assignment.strategy")]
        public string PartitionAssignmentStrategy { get { return Get("partition.assignment.strategy"); } set { this.SetObject("partition.assignment.strategy", value); } }

        /// <summary>
        ///     Client group session and failure detection timeout.
        /// </summary>
        [DataMember(Name="session.timeout.ms")]
        public int? SessionTimeoutMs { get { return GetInt("session.timeout.ms"); } set { this.SetObject("session.timeout.ms", value); } }

        /// <summary>
        ///     Group session keepalive heartbeat interval.
        /// </summary>
        [DataMember(Name="heartbeat.interval.ms")]
        public int? HeartbeatIntervalMs { get { return GetInt("heartbeat.interval.ms"); } set { this.SetObject("heartbeat.interval.ms", value); } }

        /// <summary>
        ///     Group protocol type
        /// </summary>
        [DataMember(Name="group.protocol.type")]
        public string GroupProtocolType { get { return Get("group.protocol.type"); } set { this.SetObject("group.protocol.type", value); } }

        /// <summary>
        ///     How often to query for the current client group coordinator. If the currently assigned coordinator is down the configured query interval will be divided by ten to more quickly recover in case of coordinator reassignment.
        /// </summary>
        [DataMember(Name="coordinator.query.interval.ms")]
        public int? CoordinatorQueryIntervalMs { get { return GetInt("coordinator.query.interval.ms"); } set { this.SetObject("coordinator.query.interval.ms", value); } }

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
        ///     Gets a configuration property value given a key. Returns null if 
        ///     the property has not been set.
        /// </summary>
        /// <param name="key">
        ///     The configuration property to get.
        /// </param>
        /// <returns>
        ///     The configuration property value.
        /// </returns>
        public string Get(string key)
        {
            if (this.properties.TryGetValue(key, out string val))
            {
                return val;
            }
            return null;
        }

        /// <summary>
        ///     Gets a configuration property int? value given a key.
        /// </summary>
        /// <param name="key">
        ///     The configuration property to get.
        /// </param>
        /// <returns>
        ///     The configuration property value.
        /// </returns>
        protected int? GetInt(string key)
        {
            var result = Get(key);
            if (result == null) { return null; }
            return int.Parse(result);
        }
        
        /// <summary>
        ///     Gets a configuration property bool? value given a key.
        /// </summary>
        /// <param name="key">
        ///     The configuration property to get.
        /// </param>
        /// <returns>
        ///     The configuration property value.
        /// </returns>
        protected bool? GetBool(string key)
        {
            var result = Get(key);
            if (result == null) { return null; }
            return bool.Parse(result);
        }

        /// <summary>
        ///     Gets a configuration property enum value given a key.
        /// </summary>
        /// <param name="key">
        ///     The configuration property to get.
        /// </param>
        /// <returns>
        ///     The configuration property value.
        /// </returns>
        protected object GetEnum(Type type, string key)
        {
            var result = Get(key);
            if (result == null) { return null; }
            return Enum.Parse(type, result);
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
    [DataContract]
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
    [DataContract]
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
        public bool? EnableBackgroundPoll { get { return GetBool("dotnet.producer.enable.background.poll"); } set { this.SetObject("dotnet.producer.enable.background.poll", value); } }

        /// <summary>
        ///     Specifies whether to enable notification of delivery reports. Typically
        ///     you should set this parameter to true. Set it to false for "fire and
        ///     forget" semantics and a small boost in performance.
        /// 
        ///     default: true
        /// </summary>
        public bool? EnableDeliveryReports { get { return GetBool("dotnet.producer.enable.delivery.reports"); } set { this.SetObject("dotnet.producer.enable.delivery.reports", value); } }

        /// <summary>
        ///     A comma separated list of fields that may be optionally set in delivery
        ///     reports. Disabling delivery report fields that you do not require will
        ///     improve maximum throughput and reduce memory usage. Allowed values:
        ///     key, value, timestamp, headers, all, none.
        /// 
        ///     default: all
        /// </summary>
        public string DeliveryReportFields { get { return Get("dotnet.producer.delivery.report.fields"); } set { this.SetObject("dotnet.producer.delivery.report.fields", value.ToString()); } }

        /// <summary>
        ///     Maximum number of messages allowed on the producer queue.
        /// </summary>
        [DataMember(Name="queue.buffering.max.messages")]
        public int? QueueBufferingMaxMessages { get { return GetInt("queue.buffering.max.messages"); } set { this.SetObject("queue.buffering.max.messages", value); } }

        /// <summary>
        ///     Maximum total message size sum allowed on the producer queue. This property has higher priority than queue.buffering.max.messages.
        /// </summary>
        [DataMember(Name="queue.buffering.max.kbytes")]
        public int? QueueBufferingMaxKbytes { get { return GetInt("queue.buffering.max.kbytes"); } set { this.SetObject("queue.buffering.max.kbytes", value); } }

        /// <summary>
        ///     Delay in milliseconds to wait for messages in the producer queue to accumulate before constructing message batches (MessageSets) to transmit to brokers. A higher value allows larger and more effective (less overhead, improved compression) batches of messages to accumulate at the expense of increased message delivery latency.
        /// </summary>
        [DataMember(Name="linger.ms")]
        public int? LingerMs { get { return GetInt("linger.ms"); } set { this.SetObject("linger.ms", value); } }

        /// <summary>
        ///     How many times to retry sending a failing MessageSet. **Note:** retrying may cause reordering.
        /// </summary>
        [DataMember(Name="message.send.max.retries")]
        public int? MessageSendMaxRetries { get { return GetInt("message.send.max.retries"); } set { this.SetObject("message.send.max.retries", value); } }

        /// <summary>
        ///     The backoff time in milliseconds before retrying a protocol request.
        /// </summary>
        [DataMember(Name="retry.backoff.ms")]
        public int? RetryBackoffMs { get { return GetInt("retry.backoff.ms"); } set { this.SetObject("retry.backoff.ms", value); } }

        /// <summary>
        ///     The threshold of outstanding not yet transmitted broker requests needed to backpressure the producer's message accumulator. If the number of not yet transmitted requests equals or exceeds this number, produce request creation that would have otherwise been triggered (for example, in accordance with linger.ms) will be delayed. A lower number yields larger and more effective batches. A higher value can improve latency when using compression on slow machines.
        /// </summary>
        [DataMember(Name="queue.buffering.backpressure.threshold")]
        public int? QueueBufferingBackpressureThreshold { get { return GetInt("queue.buffering.backpressure.threshold"); } set { this.SetObject("queue.buffering.backpressure.threshold", value); } }

        /// <summary>
        ///     Maximum number of messages batched in one MessageSet. The total MessageSet size is also limited by message.max.bytes.
        /// </summary>
        [DataMember(Name="batch.num.messages")]
        public int? BatchNumMessages { get { return GetInt("batch.num.messages"); } set { this.SetObject("batch.num.messages", value); } }

        /// <summary>
        ///     This field indicates how many acknowledgements the leader broker must receive from ISR brokers before responding to the request: *0*=Broker does not send any response/ack to client, *1*=Only the leader broker will need to ack the message, *-1* or *all*=broker will block until message is committed by all in sync replicas (ISRs) or broker's `min.insync.replicas` setting before sending response.
        /// </summary>
        [DataMember(Name="acks")]
        public int? Acks { get { return GetInt("acks"); } set { this.SetObject("acks", value); } }

        /// <summary>
        ///     The ack timeout of the producer request in milliseconds. This value is only enforced by the broker and relies on `request.required.acks` being != 0.
        /// </summary>
        [DataMember(Name="request.timeout.ms")]
        public int? RequestTimeoutMs { get { return GetInt("request.timeout.ms"); } set { this.SetObject("request.timeout.ms", value); } }

        /// <summary>
        ///     Local message timeout. This value is only enforced locally and limits the time a produced message waits for successful delivery. A time of 0 is infinite. This is the maximum time librdkafka may use to deliver a message (including retries). Delivery error occurs when either the retry count or the message timeout are exceeded.
        /// </summary>
        [DataMember(Name="message.timeout.ms")]
        public int? MessageTimeoutMs { get { return GetInt("message.timeout.ms"); } set { this.SetObject("message.timeout.ms", value); } }

        /// <summary>
        ///     Producer queuing strategy. FIFO preserves produce ordering, while LIFO prioritizes new messages. WARNING: `lifo` is experimental and subject to change or removal.
        /// </summary>
        [DataMember(Name="queuing.strategy")]
        public QueuingStrategyType? QueuingStrategy { get { return (QueuingStrategyType?)GetEnum(typeof(QueuingStrategyType), "queuing.strategy"); } set { this.SetObject("queuing.strategy", value); } }

        /// <summary>
        ///     Partitioner: `random` - random distribution, `consistent` - CRC32 hash of key (Empty and NULL keys are mapped to single partition), `consistent_random` - CRC32 hash of key (Empty and NULL keys are randomly partitioned), `murmur2` - Java Producer compatible Murmur2 hash of key (NULL keys are mapped to single partition), `murmur2_random` - Java Producer compatible Murmur2 hash of key (NULL keys are randomly partitioned. This is functionally equivalent to the default partitioner in the Java Producer.).
        /// </summary>
        [DataMember(Name="partitioner")]
        public string Partitioner { get { return Get("partitioner"); } set { this.SetObject("partitioner", value); } }

        /// <summary>
        ///     Compression level parameter for algorithm selected by configuration property `compression.codec`. Higher values will result in better compression at the cost of more CPU usage. Usable range is algorithm-dependent: [0-9] for gzip; [0-12] for lz4; only 0 for snappy; -1 = codec-dependent default compression level.
        /// </summary>
        [DataMember(Name="compression.level")]
        public int? CompressionLevel { get { return GetInt("compression.level"); } set { this.SetObject("compression.level", value); } }

    }


    /// <summary>
    ///     Consumer configuration properties
    /// </summary>
    [DataContract]
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
        public string ConsumeResultFields { set { this.SetObject("dotnet.consumer.consume.result.fields", value); } }

        /// <summary>
        ///     Automatically and periodically commit offsets in the background. Note: setting this to false does not prevent the consumer from fetching previously committed start offsets. To circumvent this behaviour set specific start offsets per partition in the call to assign().
        /// </summary>
        [DataMember(Name="enable.auto.commit")]
        public bool? EnableAutoCommit { get { return GetBool("enable.auto.commit"); } set { this.SetObject("enable.auto.commit", value); } }

        /// <summary>
        ///     The frequency in milliseconds that the consumer offsets are committed (written) to offset storage. (0 = disable). This setting is used by the high-level consumer.
        /// </summary>
        [DataMember(Name="auto.commit.interval.ms")]
        public int? AutoCommitIntervalMs { get { return GetInt("auto.commit.interval.ms"); } set { this.SetObject("auto.commit.interval.ms", value); } }

        /// <summary>
        ///     Automatically store offset of last message provided to application.
        /// </summary>
        [DataMember(Name="enable.auto.offset.store")]
        public bool? EnableAutoOffsetStore { get { return GetBool("enable.auto.offset.store"); } set { this.SetObject("enable.auto.offset.store", value); } }

        /// <summary>
        ///     Minimum number of messages per topic+partition librdkafka tries to maintain in the local consumer queue.
        /// </summary>
        [DataMember(Name="queued.min.messages")]
        public int? QueuedMinMessages { get { return GetInt("queued.min.messages"); } set { this.SetObject("queued.min.messages", value); } }

        /// <summary>
        ///     Maximum number of kilobytes per topic+partition in the local consumer queue. This value may be overshot by fetch.message.max.bytes. This property has higher priority than queued.min.messages.
        /// </summary>
        [DataMember(Name="queued.max.messages.kbytes")]
        public int? QueuedMaxMessagesKbytes { get { return GetInt("queued.max.messages.kbytes"); } set { this.SetObject("queued.max.messages.kbytes", value); } }

        /// <summary>
        ///     Maximum time the broker may wait to fill the response with fetch.min.bytes.
        /// </summary>
        [DataMember(Name="fetch.wait.max.ms")]
        public int? FetchWaitMaxMs { get { return GetInt("fetch.wait.max.ms"); } set { this.SetObject("fetch.wait.max.ms", value); } }

        /// <summary>
        ///     Initial maximum number of bytes per topic+partition to request when fetching messages from the broker. If the client encounters a message larger than this value it will gradually try to increase it until the entire message can be fetched.
        /// </summary>
        [DataMember(Name="max.partition.fetch.bytes")]
        public int? MaxPartitionFetchBytes { get { return GetInt("max.partition.fetch.bytes"); } set { this.SetObject("max.partition.fetch.bytes", value); } }

        /// <summary>
        ///     Maximum amount of data the broker shall return for a Fetch request. Messages are fetched in batches by the consumer and if the first message batch in the first non-empty partition of the Fetch request is larger than this value, then the message batch will still be returned to ensure the consumer can make progress. The maximum message batch size accepted by the broker is defined via `message.max.bytes` (broker config) or `max.message.bytes` (broker topic config). `fetch.max.bytes` is automatically adjusted upwards to be at least `message.max.bytes` (consumer config).
        /// </summary>
        [DataMember(Name="fetch.max.bytes")]
        public int? FetchMaxBytes { get { return GetInt("fetch.max.bytes"); } set { this.SetObject("fetch.max.bytes", value); } }

        /// <summary>
        ///     Minimum number of bytes the broker responds with. If fetch.wait.max.ms expires the accumulated data will be sent to the client regardless of this setting.
        /// </summary>
        [DataMember(Name="fetch.min.bytes")]
        public int? FetchMinBytes { get { return GetInt("fetch.min.bytes"); } set { this.SetObject("fetch.min.bytes", value); } }

        /// <summary>
        ///     How long to postpone the next fetch request for a topic+partition in case of a fetch error.
        /// </summary>
        [DataMember(Name="fetch.error.backoff.ms")]
        public int? FetchErrorBackoffMs { get { return GetInt("fetch.error.backoff.ms"); } set { this.SetObject("fetch.error.backoff.ms", value); } }

        /// <summary>
        ///     Emit RD_KAFKA_RESP_ERR__PARTITION_EOF event whenever the consumer reaches the end of a partition.
        /// </summary>
        [DataMember(Name="enable.partition.eof")]
        public bool? EnablePartitionEof { get { return GetBool("enable.partition.eof"); } set { this.SetObject("enable.partition.eof", value); } }

        /// <summary>
        ///     Verify CRC32 of consumed messages, ensuring no on-the-wire or on-disk corruption to the messages occurred. This check comes at slightly increased CPU usage.
        /// </summary>
        [DataMember(Name="check.crcs")]
        public bool? CheckCrcs { get { return GetBool("check.crcs"); } set { this.SetObject("check.crcs", value); } }

        /// <summary>
        ///     Action to take when there is no initial offset in offset store or the desired offset is out of range: 'smallest','earliest' - automatically reset the offset to the smallest offset, 'largest','latest' - automatically reset the offset to the largest offset, 'error' - trigger an error which is retrieved by consuming messages and checking 'message->err'.
        /// </summary>
        [DataMember(Name="auto.offset.reset")]
        public AutoOffsetResetType? AutoOffsetReset { get { return (AutoOffsetResetType?)GetEnum(typeof(AutoOffsetResetType), "auto.offset.reset"); } set { this.SetObject("auto.offset.reset", value); } }

    }

}
