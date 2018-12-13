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
        ///     sasl.mechanisms is an awkward case becasue the values contain '-' characters (and 
        ///     there are other values that contain the '_' character, so can't 1:1 map with this).
        ///     This type is defined by hand later.
        /// </remarks>
        internal static Dictionary<string, List<string>> AdditionalEnums => new Dictionary<string, List<string>>
        {
            { "partition.assignment.strategy", new List<string> { "range", "roundrobin" } },
            { "partitioner", new List<string> { "random", "consistent", "consistent_random", "murmur2", "murmur2_random" } } 
        };

        /// <summary>
        ///     A function that filters out properties from the librdkafka list that should
        ///     not be privided in the 
        /// </summary>
        internal static List<PropertySpecification> RemoveLegacyOrNotRelevant(List<PropertySpecification> props) 
            => props.Where(p => {
                if (p.Name == "sasl.mechanisms") { return false; } // handled as a special case.
                if (p.Name == "sasl.mechanism") { return false; } // handled as a special case.
                if (p.Name == "consume.callback.max.messages") { return false; }
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
                { "compression.type", "compression.codec" },
                { "acks", "request.required.acks" }
            };

        /// <summary>
        ///     SaslMechanismType definition
        /// </summary>
        internal static string SaslMechanismEnumString =>
@"
    /// <summary>
    ///     SaslMechanism enum values
    /// </summary>
    public enum SaslMechanismType
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
";

        /// <summary>
        ///     get/set for SaslMechanism.
        /// </summary>
        internal static string SaslMechanismGetSetString =>
@"
        /// <summary>
        ///     SASL mechanism to use for authentication. Supported: GSSAPI, PLAIN, SCRAM-SHA-256, SCRAM-SHA-512. **NOTE**: Despite the name, you may not configure more than one mechanism.
        /// </summary>
        public SaslMechanismType? SaslMechanism
        {
            get
            {
                var r = Get(""sasl.mechanism"");
                if (r == null) { return null; }
                if (r == ""GSSAPI"") { return  SaslMechanismType.Gssapi; }
                if (r == ""PLAIN"") { return SaslMechanismType.Plain; }
                if (r == ""SCRAM-SHA-256"") { return SaslMechanismType.ScramSha256; }
                if (r == ""SCRAM-SHA-512"") { return SaslMechanismType.ScramSha512; }
                throw new ArgumentException($""Unknown sasl.mechanism value {r}"");
            }
            set
            {
                if (value == null) { this.properties.Remove(""sasl.mechanism""); }
                else if (value == SaslMechanismType.Gssapi) { this.properties[""sasl.mechanism""] = ""GSSAPI""; }
                else if (value == SaslMechanismType.Plain) { this.properties[""sasl.mechanism""] = ""PLAIN""; }
                else if (value == SaslMechanismType.ScramSha256) { this.properties[""sasl.mechanism""] = ""SCRAM-SHA-256""; }
                else if (value == SaslMechanismType.ScramSha512) { this.properties[""sasl.mechanism""] = ""SCRAM-SHA-512""; }
                else throw new NotImplementedException($""Unknown sasl.mechanism value {value}"");
            }
        }

";
    }

    
    class PropertySpecification
    {
        public bool IsGlobal { get; set; }
        public string Name { get; set; }
        public string CPorA { get; set; }  // Consumer, Producer or All.
        public string Range { get; set; }
        public string Importance { get; set; }
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

        static string createFileHeader(string branch)
        {
            return
@"// *** Auto-generated from librdkafka branch " + branch + @" *** - do not modify manually.
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
                    configName.Split('.').Select(p => p[0].ToString().ToUpper() + p.Substring(1))),
                "_[a-z]",
                m => "_" + m.Value.Substring(1).ToUpper());

        static string EnumNameToDotnetName(string enumName)
            => Regex.Replace(
                enumName[0].ToString().ToUpper() + enumName.Substring(1),
                "_[a-z]",
                m => "_" + m.Value.Substring(1).ToUpper());

        static string createProperties(IEnumerable<PropertySpecification> props)
        {
            var codeText = "";
            foreach (var prop in props)
            {
                if (prop.Type == "pointer") { continue; }
                var type = (prop.Type == "enum" || MappingConfiguration.AdditionalEnums.Keys.Contains(prop.Name)) ? ConfigNameToDotnetName(prop.Name) + "Type" : prop.Type;
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
            for (int j=0; j<props.Count(); ++j)
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
                codeText += $"    public enum {ConfigNameToDotnetName(prop.Name)}Type\n";
                codeText += $"    {{\n";
                for (int i=0; i<vs.Count; ++i)
                {
                    var v = vs[i];
                    var nm = EnumNameToDotnetName(v);
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
            codeText += $"    public class {name}{(derive ? " : ClientConfig" : " : Config")}\n";
            codeText += $"    {{\n";
            return codeText;
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
        ///     <see cref=""Confluent.Kafka.Consumer.Consume(System.TimeSpan)"" />
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
                if (columns.Length != 6) { continue; }
                if (columns[0].Contains("-----")) { continue; }
                if (columns[0].Contains("Property")) { continue; }

                var prop = new PropertySpecification();
                prop.IsGlobal = parsingGlobal;
                prop.Name = columns[0].Trim();
                prop.CPorA = columns[1].Trim();
                prop.Range = columns[2].Trim();
                prop.Default = columns[3].Trim();
                prop.Importance = columns[4].Trim();

                var desc = columns[5].Trim();
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

            var props =
                choosePreferredNames(
                linkAliased(
                removeDuplicateTopicLevel(
                MappingConfiguration.RemoveLegacyOrNotRelevant(
                extractAll(configDoc)))));

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
            codeText += createClassHeader("ClientConfig", "Configuration common to all clients", false);
            codeText += MappingConfiguration.SaslMechanismGetSetString;
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
