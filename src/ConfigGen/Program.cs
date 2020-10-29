using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading.Tasks;


namespace ConfigGen
{
    internal class MappingConfiguration
    {
        /// <summary>
        ///     librdkafka _RK_C_S2I properties are automatically interpreted as enums, however
        ///     _RK_C_STR properties with discrete set of allowed values are not. Enum values for
        ///     these property types are specified here.
        /// </summary>
        /// <remarks>
        ///     sasl.mechanisms is an awkward case because the values contain '-' characters (and
        ///     there are other values that contain the '_' character, so can't 1:1 map with this).
        ///     This type is defined by hand later.
        /// </remarks>
        internal static Dictionary<string, List<string>> AdditionalEnums => new Dictionary<string, List<string>>
        {
            { "partition.assignment.strategy", new List<string> { "range", "roundrobin", "cooperative-sticky" } },
            { "partitioner", new List<string> { "random", "consistent", "consistent_random", "murmur2", "murmur2_random" } }
        };

        /// <summary>
        ///     A function that filters out properties from the librdkafka list that should
        ///     not be automatically extracted.
        /// </summary>
        internal static List<PropertySpecification> RemoveLegacyOrNotRelevant(List<PropertySpecification> props)
            => props.Where(p => {
                // handled as a special case.
                if (p.Name == "sasl.mechanisms") { return false; }
                if (p.Name == "sasl.mechanism") { return false; }
                if (p.Name == "acks") { return false; }
                if (p.Name == "request.required.acks") { return false; }
                // legacy
                if (p.Name == "consume.callback.max.messages") { return false; }
                if (p.Name == "offset.store.method") { return false; }
                if (p.Name == "offset.store.path") { return false; }
                if (p.Name == "offset.store.sync.interval.ms") { return false; }
                if (p.Name == "builtin.features") { return false; }
                if (p.Name == "produce.offset.report") { return false; }
                if (p.Name == "delivery.report.only.error") { return false; }
                if (p.Name == "topic.metadata.refresh.fast.cnt") { return false; }
                if (p.Name == "reconnect.backoff.jitter.ms") { return false; }
                if (p.Name == "socket.blocking.max.ms") { return false; }
                if (p.Name == "auto.commit.interval.ms" && !p.IsGlobal) { return false; }
                if (p.Name == "enable.auto.commit" && !p.IsGlobal) { return false; }
                if (p.Name == "auto.commit.enable" && !p.IsGlobal) { return false; }
                if (p.Name == "queuing.strategy") { return false; }
                // other
                if (p.Name.Contains("_")) { return false; }
                return true;
            }).ToList();

        /// <summary>
        ///     A dictionary of synonym config properties. The key is included in the config
        ///     classes, the value is not.
        /// </summary>
        internal static Dictionary<string, string> PreferredNames =>
            new Dictionary<string, string>
            {
                { "bootstrap.servers", "metadata.broker.list" },
                { "max.in.flight", "max.in.flight.requests.per.connection" },
                { "max.partition.fetch.bytes", "fetch.message.max.bytes" },
                { "linger.ms", "queue.buffering.max.ms" },
                { "message.send.max.retries", "retries" },
                { "compression.type", "compression.codec" }
            };

        /// <summary>
        ///     SaslMechanism definition
        /// </summary>
        internal static string SaslMechanismEnumString =>
@"
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
        ScramSha512,

        /// <summary>
        ///     OAUTHBEARER
        /// </summary>
        OAuthBearer
    }
";

        /// <summary>
        ///     get/set for SaslMechanism.
        /// </summary>
        internal static string SaslMechanismGetSetString =>
@"
        /// <summary>
        ///     SASL mechanism to use for authentication. Supported: GSSAPI, PLAIN, SCRAM-SHA-256, SCRAM-SHA-512. **NOTE**: Despite the name, you may not configure more than one mechanism.
        /// </summary>
        public SaslMechanism? SaslMechanism
        {
            get
            {
                var r = Get(""sasl.mechanism"");
                if (r == null) { return null; }
                if (r == ""GSSAPI"") { return Confluent.Kafka.SaslMechanism.Gssapi; }
                if (r == ""PLAIN"") { return Confluent.Kafka.SaslMechanism.Plain; }
                if (r == ""SCRAM-SHA-256"") { return Confluent.Kafka.SaslMechanism.ScramSha256; }
                if (r == ""SCRAM-SHA-512"") { return Confluent.Kafka.SaslMechanism.ScramSha512; }
                if (r == ""OAUTHBEARER"") { return Confluent.Kafka.SaslMechanism.OAuthBearer; }
                throw new ArgumentException($""Unknown sasl.mechanism value {r}"");
            }
            set
            {
                if (value == null) { this.properties.Remove(""sasl.mechanism""); }
                else if (value == Confluent.Kafka.SaslMechanism.Gssapi) { this.properties[""sasl.mechanism""] = ""GSSAPI""; }
                else if (value == Confluent.Kafka.SaslMechanism.Plain) { this.properties[""sasl.mechanism""] = ""PLAIN""; }
                else if (value == Confluent.Kafka.SaslMechanism.ScramSha256) { this.properties[""sasl.mechanism""] = ""SCRAM-SHA-256""; }
                else if (value == Confluent.Kafka.SaslMechanism.ScramSha512) { this.properties[""sasl.mechanism""] = ""SCRAM-SHA-512""; }
                else if (value == Confluent.Kafka.SaslMechanism.OAuthBearer) { this.properties[""sasl.mechanism""] = ""OAUTHBEARER""; }
                else throw new ArgumentException($""Unknown sasl.mechanism value {value}"");
            }
        }

";


        /// <summary>
        ///     SaslMechanism definition
        /// </summary>
        internal static string AcksEnumString =>
@"
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
";

        /// <summary>
        ///     get/set for Acks.
        /// </summary>
        internal static string AcksGetSetString =>
@"
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
                var r = Get(""acks"");
                if (r == null) { return null; }
                if (r == ""0"") { return Confluent.Kafka.Acks.None; }
                if (r == ""1"") { return Confluent.Kafka.Acks.Leader; }
                if (r == ""-1"" || r == ""all"") { return Confluent.Kafka.Acks.All; }
                return (Acks)(int.Parse(r));
            }
            set
            {
                if (value == null) { this.properties.Remove(""acks""); }
                else if (value == Confluent.Kafka.Acks.None) { this.properties[""acks""] = ""0""; }
                else if (value == Confluent.Kafka.Acks.Leader) { this.properties[""acks""] = ""1""; }
                else if (value == Confluent.Kafka.Acks.All) { this.properties[""acks""] = ""-1""; }
                else { this.properties[""acks""] = ((int)value.Value).ToString(); }
            }
        }

";

    }


    class PropertySpecification : IComparable
    {
        public PropertySpecification() {}

        public PropertySpecification(PropertySpecification other)
        {
            IsGlobal = other.IsGlobal;
            Name = other.Name;
            CPorA = other.CPorA;
            Range = other.Range;
            Importance = other.Importance;
            Default = other.Default;
            Description = other.Description;
            Type = other.Type;
            AliasFor = other.AliasFor;
        }

        public bool IsGlobal { get; set; }
        public string Name { get; set; }
        public string CPorA { get; set; }  // Consumer, Producer or All.
        public string Range { get; set; }
        public string Importance { get; set; }
        public string Default { get; set; }
        public string Description { get; set; }
        public string Type { get; set; }
        public string AliasFor { get; set; }

        public int CompareTo(object obj)
            => Name.CompareTo(((PropertySpecification)obj).Name);
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
            if (type == "float") { return "double"; }
            if (type == "pointer") { return "pointer"; }
            if (type == "") { return "pointer"; }
            if (type == "see dedicated API") { return "pointer"; }
            throw new Exception($"unknown type '{type}'");
        }

        static string createFileHeader(string branch)
        {
            return
@"// *** Auto-generated from librdkafka " + branch + @" *** - do not modify manually.
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
            => Regex.Replace(
                string.Concat(
                    configName.Split('.').Select(p => char.ToUpper(p[0]) + p.Substring(1))),
                "_[a-z]",
                m => "_" + m.Value.Substring(1).ToUpper());

        private static Dictionary<string, string> ConfigValueToEnumNameSubstitutes = new Dictionary<string, string>
        {
            { "sasl_plaintext", "SaslPlaintext" },
            { "sasl_ssl", "SaslSsl" },
            { "consistent_random", "ConsistentRandom" },
            { "murmur2_random", "Murmur2Random"},
            { "roundrobin", "RoundRobin" },
            { "cooperative-sticky", "CooperativeSticky"},
            { "read_uncommitted", "ReadUncommitted" },
            { "read_committed", "ReadCommitted" }
        };

        static string EnumNameToDotnetName(string enumName)
        {
            if (ConfigValueToEnumNameSubstitutes.TryGetValue(enumName, out string substitute))
            {
                return substitute;
            }

            var result = char.ToUpper(enumName[0]) + enumName.Substring(1);
            if (result.Contains('_'))
            {
                Console.WriteLine($"warning: enum value contains underscore (is not consistent with .net naming standards): {enumName}");
            }

            return result;
        }

        static string createProperties(IEnumerable<PropertySpecification> props)
        {
            var codeText = "";
            foreach (var prop in props)
            {
                if (prop.Type == "pointer") { continue; }
                var type = (prop.Type == "enum" || MappingConfiguration.AdditionalEnums.Keys.Contains(prop.Name)) ? ConfigNameToDotnetName(prop.Name) : prop.Type;
                var nullableType = type == "string" ? "string" : type + "?";

                codeText += $"        /// <summary>\n";
                codeText += $"        ///     {prop.Description}\n";
                codeText += $"        ///\n";
                codeText += $"        ///     default: {(prop.Default == "" ? "''" : prop.Default)}\n";
                codeText += $"        ///     importance: {prop.Importance}\n";
                codeText += $"        /// </summary>\n";
                codeText += $"        public {nullableType} {ConfigNameToDotnetName(prop.Name)} {{ get {{ return ";
                switch (type)
                {
                    case "string":
                        codeText += $"Get(\"{prop.Name}\")";
                        break;
                    case "int":
                        codeText += $"GetInt(\"{prop.Name}\")";
                        break;
                    case "bool":
                        codeText += $"GetBool(\"{prop.Name}\")";
                        break;
                    case "double":
                        codeText += $"GetDouble(\"{prop.Name}\")";
                        break;
                    default:
                        codeText += $"({nullableType})GetEnum(typeof({type}), \"{prop.Name}\")";
                        break;
                }
                codeText += $"; }} set {{ this.SetObject(\"{prop.Name}\", value); }} }}\n";
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
            for (int j = 0; j < props.Count(); ++j)
            {
                var prop = props[j];
                List<string> vs = null;
                if (prop.Type == "string")
                {
                    vs = MappingConfiguration.AdditionalEnums[prop.Name];
                }
                else
                {
                    vs = prop.Range.Split(',').Select(v => v.Trim()).ToList();
                    if (prop.Name == "auto.offset.reset")
                    {
                        // Only expose the options allowed by the Java client.
                        vs = new List<string> { "Latest", "Earliest", "Error" };
                    }
                }
                if (j != 0) { codeText += "\n"; }
                codeText += $"    /// <summary>\n";
                codeText += $"    ///     {ConfigNameToDotnetName(prop.Name)} enum values\n";
                codeText += $"    /// </summary>\n";
                codeText += $"    public enum {ConfigNameToDotnetName(prop.Name)}\n";
                codeText += $"    {{\n";
                for (int i = 0; i < vs.Count; ++i)
                {
                    var v = vs[i];
                    var nm = EnumNameToDotnetName(v);
                    codeText += $"        /// <summary>\n";
                    codeText += $"        ///     {nm}\n";
                    codeText += $"        /// </summary>\n";
                    codeText += $"        {nm}{(i == vs.Count - 1 ? "" : ",\n")}\n";
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
            codeText += $"    public class {name}{(derive ? " : ClientConfig" : " : Config")}\n";
            codeText += $"    {{\n";
            return codeText;
        }

        static string createClassConstructors(string name)
        {
            var codeText = $@"
        /// <summary>
        ///     Initialize a new empty <see cref=""{name}"" /> instance.
        /// </summary>
        public {name}() : base() {{ }}

        /// <summary>
        ///     Initialize a new <see cref=""{name}"" /> instance wrapping
        ///     an existing <see cref=""ClientConfig"" /> instance.
        ///     This will change the values ""in-place"" i.e. operations on this class WILL modify the provided collection
        /// </summary>
        public {name}(ClientConfig config) : base(config) {{ }}

        /// <summary>
        ///     Initialize a new <see cref=""{name}"" /> instance wrapping
        ///     an existing key/value pair collection.
        ///     This will change the values ""in-place"" i.e. operations on this class WILL modify the provided collection
        /// </summary>
        public {name}(IDictionary<string, string> config) : base(config) {{ }}
";
            return codeText;
        }

        static string createConsumerSpecific()
        {
            return
                createClassConstructors("ConsumerConfig") +
@"
        /// <summary>
        ///     A comma separated list of fields that may be optionally set
        ///     in <see cref=""Confluent.Kafka.ConsumeResult{TKey,TValue}"" />
        ///     objects returned by the
        ///     <see cref=""Confluent.Kafka.Consumer{TKey,TValue}.Consume(System.TimeSpan)"" />
        ///     method. Disabling fields that you do not require will improve
        ///     throughput and reduce memory consumption. Allowed values:
        ///     headers, timestamp, topic, all, none
        ///
        ///     default: all
        ///     importance: low
        /// </summary>
        public string ConsumeResultFields { set { this.SetObject(""dotnet.consumer.consume.result.fields"", value); } }

";
        }

        static string createProducerSpecific()
        {
            return
                createClassConstructors("ProducerConfig") +
@"
        /// <summary>
        ///     Specifies whether or not the producer should start a background poll
        ///     thread to receive delivery reports and event notifications. Generally,
        ///     this should be set to true. If set to false, you will need to call
        ///     the Poll function manually.
        ///
        ///     default: true
        ///     importance: low
        /// </summary>
        public bool? EnableBackgroundPoll { get { return GetBool(""dotnet.producer.enable.background.poll""); } set { this.SetObject(""dotnet.producer.enable.background.poll"", value); } }

        /// <summary>
        ///     Specifies whether to enable notification of delivery reports. Typically
        ///     you should set this parameter to true. Set it to false for ""fire and
        ///     forget"" semantics and a small boost in performance.
        ///
        ///     default: true
        ///     importance: low
        /// </summary>
        public bool? EnableDeliveryReports { get { return GetBool(""dotnet.producer.enable.delivery.reports""); } set { this.SetObject(""dotnet.producer.enable.delivery.reports"", value); } }

        /// <summary>
        ///     A comma separated list of fields that may be optionally set in delivery
        ///     reports. Disabling delivery report fields that you do not require will
        ///     improve maximum throughput and reduce memory usage. Allowed values:
        ///     key, value, timestamp, headers, all, none.
        ///
        ///     default: all
        ///     importance: low
        /// </summary>
        public string DeliveryReportFields { get { return Get(""dotnet.producer.delivery.report.fields""); } set { this.SetObject(""dotnet.producer.delivery.report.fields"", value.ToString()); } }

";
        }

        static string createAdminClientSpecific()
        {
            return createClassConstructors("AdminClientConfig");
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

                var columns = SplitLine(line).ToArray();
                if (columns.Length != 6) { continue; }
                if (columns[0].Contains("-----")) { continue; }
                if (columns[0].Contains("Property")) { continue; }

                var prop = new PropertySpecification();
                prop.IsGlobal = parsingGlobal;
                prop.Name = columns[0];
                prop.CPorA = columns[1];
                prop.Range = columns[2];
                prop.Default = columns[3].Replace("\\|", "|");
                prop.Importance = columns[4];

                var desc = columns[5].Replace("\\|", "|");
                bool isAlias = desc.StartsWith("Alias");
                if (isAlias)
                {
                    var firstIdx = desc.IndexOf('`') + 1;
                    prop.AliasFor = desc.Substring(firstIdx, desc.IndexOf('`', firstIdx) - desc.IndexOf('`') - 1);
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

        static IEnumerable<string> SplitLine(string line)
        {
            if (string.IsNullOrWhiteSpace(line))
                yield break;

            int lastPipe = 0;
            for (int i = 1; i < line.Length - 1; i++)
            {
                if (line[i] == '|' && line[i - 1] == ' ' && line[i + 1] == ' ')
                {
                    yield return line.Substring(lastPipe, i - lastPipe).Trim();
                    lastPipe = i + 1;
                }
            }
            yield return line.Substring(lastPipe + 1).Trim();
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
            props = topicLevel.Where(p => !removeTopicLevel.Contains(p.Name)).Concat(global).ToList();
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
            return props.Select(p => {
                if (p.AliasFor != null && MappingConfiguration.PreferredNames.ContainsKey(p.AliasFor))
                {
                    var af = p.AliasFor;
                    var n = p.Name;
                    p.Name = af;
                    p.AliasFor = n;
                }
                return p;
            }).ToList();
        }

        static void PrintProps(IEnumerable<PropertySpecification> props)
        {
            var props_ = props.ToArray();
            Array.Sort(props_);
            Console.WriteLine(String.Join("  ", props_.Select(p => p.Name)));
        }

        static async Task<int> Main(string[] args)
        {
            if (args.Length != 1)
            {
                Console.WriteLine("usage: .. git-branch-name");
                return 1;
            }

            string gitBranchName = args[0];
            string url = $"https://raw.githubusercontent.com/edenhill/librdkafka/{gitBranchName}/CONFIGURATION.md";
            var configDoc = await (await (new HttpClient())
                .GetAsync(url))
                .Content.ReadAsStringAsync();

            var props = extractAll(configDoc);
            var props2 = MappingConfiguration.RemoveLegacyOrNotRelevant(props);
            var props3 = removeDuplicateTopicLevel(props2);
            var props4 = props = linkAliased(props3);
            var props5 = choosePreferredNames(props4);

            if (props.Count() == 0)
            {
                Console.WriteLine($"no properties found at url: {url}");
                return 1;
            }

            Console.WriteLine($"property counts: [all: {props.Count()}, *: {props.Where(p => p.CPorA == "*").Count()}, C: {props.Where(p => p.CPorA == "C").Count()}, P: {props.Where(p => p.CPorA == "P").Count()}].");

            var codeText = "";
            codeText += createFileHeader(gitBranchName);
            codeText += createEnums(props.Where(p => p.Type == "enum" || MappingConfiguration.AdditionalEnums.Keys.Contains(p.Name)).ToList());
            codeText += MappingConfiguration.SaslMechanismEnumString;
            codeText += MappingConfiguration.AcksEnumString;
            codeText += createClassHeader("ClientConfig", "Configuration common to all clients", false);
            codeText += createClassConstructors("ClientConfig");
            codeText += MappingConfiguration.SaslMechanismGetSetString;
            codeText += MappingConfiguration.AcksGetSetString;
            codeText += createProperties(props.Where(p => p.CPorA == "*"));
            codeText += createClassFooter();
            codeText += createClassHeader("AdminClientConfig", "AdminClient configuration properties", true);
            codeText += createAdminClientSpecific();
            codeText += createClassFooter();
            codeText += createClassHeader("ProducerConfig", "Producer configuration properties", true);
            codeText += createProducerSpecific();
            codeText += createProperties(props.Where(p => p.CPorA == "P"));
            codeText += createClassFooter();
            codeText += createClassHeader("ConsumerConfig", "Consumer configuration properties", true);
            codeText += createConsumerSpecific();
            codeText += createProperties(props.Where(p => p.CPorA == "C"));
            codeText += createClassFooter();
            codeText += createFileFooter();

            if (!Directory.Exists("out")) { Directory.CreateDirectory("out"); }
            File.WriteAllText("out/Config_gen.cs", codeText);

            return 0;
        }
    }
}
