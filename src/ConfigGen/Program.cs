using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;

// todo: don't expose all enum values where they are aliases.

namespace ConfigGen
{
    class PropertySpecification
    {
        public bool IsGlobal { get; set; }
        public string Name { get; set; }
        public string CPA { get; set; }
        public string Range { get; set; }
        public string Default { get; set; }
        public string Description { get; set; }
        public string Type { get; set; }
        public string AliasFor { get; set; }
    }

    class Program
    {
        static string parseType(string type)
        {
            if (type == "string") { return "string"; }
            if (type == "integer") { return "int"; }
            if (type == "boolean") { return "bool"; }
            if (type == "enum value") { return "enum"; }
            if (type == "CSV flags") { return "string"; }
            if (type == "pattern list") { return "string"; }
            if (type == "pointer") { return "pointer"; }
            if (type == "") { return "pointer"; }
            throw new Exception($"unknown type '{type}'");
        }

        static string createFileHeader()
        {
            return
@"// *** Auto-generated *** - do not modify manually.
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
";
        }

        static string createFileFooter()
        {
            return
@"}
";
        }

        static string ConfigNameToDotnetName(string configName)
        {
            return string.Concat(configName.Split('.').Select(p => p[0].ToString().ToUpper() + p.Substring(1)));
        }

        static string createProperties(IEnumerable<PropertySpecification> props)
        {
            var codeText = "";
            foreach (var prop in props)
            {
                if (prop.Type == "pointer") { continue; }
                var type = prop.Type == "enum" ? ConfigNameToDotnetName(prop.Name) + "Type" : prop.Type;

                codeText += $"        /// <summary>\n";
                codeText += $"        ///     {prop.Description}\n";
                codeText += $"        /// </summary>\n";
                codeText += $"        public {type} {ConfigNameToDotnetName(prop.Name)} {{ set {{ this.SetObject(\"{prop.Name}\", value); }} }}\n";
                codeText += $"\n";
            }
            return codeText;
        }

        static string createClassFooter()
        {
            return
@"    }

";
        }

        static string createEnums(List<PropertySpecification> props)
        {
            var codeText = "";
            for (int j=0; j<props.Count(); ++j)
            {
                var prop = props[j];
                var vs = prop.Range.Split(',').Select(v => v.Trim()).ToList();
                if (prop.Name == "auto.offset.reset")
                {
                    // Only expose the options allowed by the Java client.
                    vs = new List<string> { "Latest", "Earliest", "Error" };
                }
                if (j != 0) { codeText += "\n"; }
                codeText += $"    /// <summary>\n";
                codeText += $"    ///     {ConfigNameToDotnetName(prop.Name)} enum values\n";
                codeText += $"    /// </summary>\n";
                codeText += $"    public enum {ConfigNameToDotnetName(prop.Name)}Type\n";
                codeText += $"    {{\n";
                for (int i=0; i<vs.Count; ++i)
                {
                    var v = vs[i];
                    var nm = v[0].ToString().ToUpper() + v.Substring(1);
                    codeText += $"        /// <summary>\n";
                    codeText += $"        ///     {nm}\n";
                    codeText += $"        /// </summary>\n";
                    codeText += $"        {nm}{(i == vs.Count-1 ? "" : ",\n")}\n";
                }
                codeText += $"    }}\n";
            }
            return codeText;
        }

        static string createClassHeader(string name, string docs, bool derive)
        {
            var codeText = "\n";
            codeText += $"    /// <summary>\n";
            codeText += $"    ///     {docs}\n";
            codeText += $"    /// </summary>\n";
            codeText += $"    public class {name}{(derive ? " : ClientConfig" : " : IEnumerable<KeyValuePair<string, string>>")}\n";
            codeText += $"    {{\n";
            return codeText;
        }

        static string createCallbacks()
        {
            return
@"        /// <summary>
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
        ///     <see cref=""Confluent.Kafka.Consumer{TKey, TValue}.Consume(System.Threading.CancellationToken)"" />
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
        ///     <see cref=""Confluent.Kafka.Consumer{TKey, TValue}.Consume(System.Threading.CancellationToken)"" />
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
        /// <param name=""key"">
        ///     The configuration property name.
        /// </param>
        /// <param name=""val"">
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
                throw new ArgumentException($""value for property {name} cannot be null."");
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
";
        }

        static string createClientSpecific()
        {
            return
@"        /// <summary>
        ///     Initialize a new empty <see cref=""ClientConfig"" /> instance.
        /// </summary>
        public ClientConfig() {}

        /// <summary>
        ///     Initialize a new <see cref=""ClientConfig"" /> instance based on
        ///     an existing <see cref=""ClientConfig"" /> instance.
        /// </summary>
        public ClientConfig(ClientConfig config) { this.properties = new Dictionary<string, string>(config.ToDictionary(a => a.Key, a => a.Value)); }

        /// <summary>
        ///     Initialize a new <see cref=""ClientConfig"" /> instance based on
        ///     an existing key/value pair collection.
        /// </summary>
        public ClientConfig(IEnumerable<KeyValuePair<string, string>> config) { this.properties = new Dictionary<string, string>(config.ToDictionary(a => a.Key, a => a.Value)); }
";
        }

        static string createConsumerSpecific()
        {
            return
@"        /// <summary>
        ///     Initialize a new empty <see cref=""ConsumerConfig"" /> instance.
        /// </summary>
        public ConsumerConfig() {}

        /// <summary>
        ///     Initialize a new <see cref=""ConsumerConfig"" /> instance based on
        ///     an existing <see cref=""ClientConfig"" /> instance.
        /// </summary>
        public ConsumerConfig(ClientConfig config) { this.properties = new Dictionary<string, string>(config.ToDictionary(a => a.Key, a => a.Value)); }

        /// <summary>
        ///     Initialize a new <see cref=""ConsumerConfig"" /> instance based on
        ///     an existing key/value pair collection.
        /// </summary>
        public ConsumerConfig(IEnumerable<KeyValuePair<string, string>> config) { this.properties = new Dictionary<string, string>(config.ToDictionary(a => a.Key, a => a.Value)); }

        /// <summary>
        ///     A comma separated list of fields that may be optionally set
        ///     in <see cref=""Confluent.Kafka.ConsumeResult{TKey, TValue}"" />
        ///     objects returned by the
        ///     <see cref=""Confluent.Kafka.Consumer{TKey, TValue}.Consume(System.TimeSpan)"" />
        ///     method. Disabling fields that you do not require will improve 
        ///     throughput and reduce memory consumption. Allowed values:
        ///     headers, timestamp, topic, all, none
        /// 
        ///     default: all
        /// </summary>
        public string ConsumeResultFields { set { this.properties[""dotnet.consumer.consume.result.fields""] = value; } }

";
        }

        static string createProducerSpecific()
        {
            return
@"        /// <summary>
        ///     Initialize a new empty <see cref=""ProducerConfig"" /> instance.
        /// </summary>
        public ProducerConfig() {}

        /// <summary>
        ///     Initialize a new <see cref=""ProducerConfig"" /> instance based on
        ///     an existing <see cref=""ClientConfig"" /> instance.
        /// </summary>
        public ProducerConfig(ClientConfig config) { this.properties = new Dictionary<string, string>(config.ToDictionary(a => a.Key, a => a.Value)); }

        /// <summary>
        ///     Initialize a new <see cref=""ProducerConfig"" /> instance based on
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
        public bool EnableBackgroundPoll { set { this.properties[""dotnet.producer.enable.background.poll""] = value.ToString(); } }

        /// <summary>
        ///     Specifies whether to enable notification of delivery reports. Typically
        ///     you should set this parameter to true. Set it to false for ""fire and
        ///     forget"" semantics and a small boost in performance.
        /// 
        ///     default: true
        /// </summary>
        public bool EnableDeliveryReports { set { this.properties[""dotnet.producer.enable.delivery.reports""] = value.ToString(); } }

        /// <summary>
        ///     A comma separated list of fields that may be optionally set in delivery
        ///     reports. Disabling delivery report fields that you do not require will
        ///     improve maximum throughput and reduce memory usage. Allowed values:
        ///     key, value, timestamp, headers, all, none.
        /// 
        ///     default: all
        /// </summary>
        public string DeliveryReportFields { set { this.properties[""dotnet.producer.delivery.report.fields""] = value.ToString(); } }

";
        }

        static string createAdminClientSpecific()
        {
            return 
@"        /// <summary>
        ///     Initialize a new empty <see cref=""AdminClientConfig"" /> instance.
        /// </summary>
        public AdminClientConfig() {}

        /// <summary>
        ///     Initialize a new <see cref=""AdminClientConfig"" /> instance based on
        ///     an existing <see cref=""ClientConfig"" /> instance.
        /// </summary>
        public AdminClientConfig(ClientConfig config) { this.properties = new Dictionary<string, string>(config.ToDictionary(a => a.Key, a => a.Value)); }

        /// <summary>
        ///     Initialize a new <see cref=""AdminClientConfig"" /> instance based on
        ///     an existing key/value pair collection.
        /// </summary>
        public AdminClientConfig(IEnumerable<KeyValuePair<string, string>> config) { this.properties = new Dictionary<string, string>(config.ToDictionary(a => a.Key, a => a.Value)); }
";
        }

        static List<PropertySpecification> extractAll(string configDoc)
        {
            var configLines = configDoc.Split('\n');

            var props = new List<PropertySpecification>();

            bool parsingGlobal = true;
            foreach (var line in configLines)
            {
                if (line.Contains("Topic configuration properties"))
                {
                    parsingGlobal = false;
                    continue;
                }

                var columns = line.Split('|');
                if (columns.Length != 5) { continue; }
                if (columns[0].Contains("-----")) { continue; }
                if (columns[0].Contains("Property")) { continue; }

                var prop = new PropertySpecification();
                prop.IsGlobal = parsingGlobal;
                prop.Name = columns[0].Trim();
                prop.CPA = columns[1].Trim();
                prop.Range = columns[2].Trim();
                prop.Default = columns[3].Trim();

                var desc = columns[4].Trim();
                bool isAlias = desc.StartsWith("Alias");
                if (isAlias != !desc.Contains("<br>*Type")) { throw new Exception("Inconsistent indication of alias parameter"); }
                if (isAlias)
                {
                    prop.AliasFor = desc.Substring(desc.IndexOf('`')+1, desc.LastIndexOf('`') - desc.IndexOf('`') - 1);
                }
                else
                {
                    string typePrefix = "<br>*Type: ";
                    if (desc.IndexOf(typePrefix) == -1) { throw new Exception($"Unexpected config description: {desc}"); }
                    prop.Description = desc.Substring(0, desc.IndexOf(typePrefix)).Trim();
                    var beginIdx = desc.IndexOf(typePrefix) + typePrefix.Length;
                    prop.Type = parseType(desc.Substring(beginIdx, desc.LastIndexOf("*") - beginIdx));
                }

                props.Add(prop);
            }

            return props;
        }

        static List<PropertySpecification> removeLegacyOrNotRelevant(List<PropertySpecification> props)
        {
            return props.Where(p => {
                if (p.Name == "consume.callback.max.messages") { return false; }
                if (p.Name == "offset.store.path") { return false; }
                if (p.Name == "offset.store.sync.interval.ms") { return false; }
                if (p.Name == "enabled_events") { return false; }
                if (p.Name == "builtin.features") { return false; }
                if (p.Name == "produce.offset.report") { return false; }
                if (p.Name == "delivery.report.only.error") { return false; }
                if (p.Name == "topic.metadata.refresh.fast.cnt") { return false; }
                if (p.Name == "auto.commit.interval.ms" && !p.IsGlobal) { return false; }
                if (p.Name == "enable.auto.commit" && !p.IsGlobal) { return false; }
                if (p.Name == "auto.commit.enable" && !p.IsGlobal) { return false; }
                return true;
            }).ToList();
        }

        static List<PropertySpecification> removeDuplicateTopicLevel(List<PropertySpecification> props)
        {
            // remove topicLevel properties that are in both topic level and global.
            var global = props.Where(p => p.IsGlobal).ToList();
            var topicLevel = props.Where(p => !p.IsGlobal).ToList();
            var removeTopicLevel = new List<string>();
            foreach (var p in topicLevel)
            {
                if (global.Count(gp => gp.Name.Equals(p.Name)) > 0) { removeTopicLevel.Add(p.Name); }
            }
            props = props.Where(p => !removeTopicLevel.Contains(p.Name)).ToList();
            return props;
        }

        static List<PropertySpecification> linkAliased(List<PropertySpecification> props)
        {
            // link up aliased properties.
            var nonAlias = props.Where(p => p.AliasFor == null).ToList();
            var aliases = props.Where(p => p.AliasFor != null).ToList();
            foreach (var alias in aliases)
            {
                var toUpdate = nonAlias.Single(p => p.Name == alias.AliasFor && p.IsGlobal == alias.IsGlobal);
                if (toUpdate.AliasFor != null) { throw new Exception("detected more than on alias for a property, not supported."); }
                toUpdate.AliasFor = alias.Name;
            }
            props = nonAlias.ToList();
            return props;
        }

        static List<PropertySpecification> choosePreferredNames(List<PropertySpecification> props)
        {
            // key -> preferred name.
            var preferredNames = new Dictionary<string, string>
            {
                { "bootstrap.servers", "metadata.broker.list" },
                { "max.in.flight", "max.in.flight.requests.per.connection" },
                { "sasl.mechanism", "sasl.mechanisms" },
                { "max.partition.fetch.bytes", "fetch.message.max.bytes" },
                { "linger.ms", "queue.buffering.max.ms" },
                { "message.send.max.retries", "retries" },
                { "compression.type", "compression.codec" },
                { "acks", "request.required.acks" }
            };
            return props.Select(p => {
                if (p.AliasFor != null && preferredNames.ContainsKey(p.AliasFor))
                {
                    var af = p.AliasFor;
                    var n = p.Name;
                    p.Name = af;
                    p.AliasFor = n;
                }
                return p;
            }).ToList();
        }

        static async Task<int> Main(string[] args)
        {
            if (args.Length != 1)
            {
                Console.WriteLine("usage: .. git-branch-name");
                return 1;
            }

            string gitBranchName = args[0];
            var configDoc = await (await (new HttpClient()).GetAsync($"https://raw.githubusercontent.com/edenhill/librdkafka/{gitBranchName}/CONFIGURATION.md")).Content.ReadAsStringAsync();

            var props =
                choosePreferredNames(
                linkAliased(
                removeDuplicateTopicLevel(
                removeLegacyOrNotRelevant(
                extractAll(configDoc)))));

            var codeText = "";
            codeText += createFileHeader();
            codeText += createEnums(props.Where(p => p.Type == "enum").ToList());
            codeText += createClassHeader("ClientConfig", "Configuration common to all clients", false);
            codeText += createClientSpecific();
            codeText += createProperties(props.Where(p => p.CPA == "*"));
            codeText += createCallbacks();
            codeText += createClassFooter();
            codeText += createClassHeader("AdminClientConfig", "AdminClient configuration properties", true);
            codeText += createAdminClientSpecific();
            codeText += createClassFooter();
            codeText += createClassHeader("ProducerConfig", "Producer configuration properties", true);
            codeText += createProducerSpecific();
            codeText += createProperties(props.Where(p => p.CPA == "P"));
            codeText += createClassFooter();
            codeText += createClassHeader("ConsumerConfig", "Consumer configuration properties", true);
            codeText += createConsumerSpecific();
            codeText += createProperties(props.Where(p => p.CPA == "C"));
            codeText += createClassFooter();
            codeText += createFileFooter();

            if (!Directory.Exists("out")) { Directory.CreateDirectory("out"); }
            File.WriteAllText("out/Config.cs", codeText);

            return 0;
        }
    }
}
