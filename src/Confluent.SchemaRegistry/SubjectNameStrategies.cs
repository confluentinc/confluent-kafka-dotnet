// Copyright 2016-2018 Confluent Inc.
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
// Refer to LICENSE for more information.

using System;
using System.Collections.Generic;

namespace Confluent.SchemaRegistry
{

    /// <summary>
    /// Keeps a map of ISubjectNameStrategy implementations
    /// </summary>
    public static class SubjectNameStrategies
    {
        /// <summary>
        /// The default subject name strategy.
        /// </summary>
        public static readonly ISubjectNameStrategy Default = new TopicSubjectNameStrategy();
        
        private static Dictionary<string, ISubjectNameStrategy> map = new Dictionary<string, ISubjectNameStrategy>
        {
            {"topic_name_strategy", Default},
            {"record_name_strategy", new RecordSubjectNameStrategy()},
            {"topic_record_name_strategy", new TopicRecordSubjectNameStrategy()}
        };

        /// <summary>
        /// Returns the ISubjectNameStrategy for the given name
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentException">If the ISubjectNameStrategy is not found</exception>
        public static ISubjectNameStrategy GetSubjectNameStrategy(string name)
        {
            if (map.ContainsKey(name))
            {
                return map[name];
            }
            throw new ArgumentException($"SubjectNameStrategy for name {name} is not supported.");
        }

        /// <summary>
        /// Allow other ISubjectNameStrategies to be used.
        /// </summary>
        /// <param name="name">The name used to look up the SubjectNameStrategy</param>
        /// <param name="strategy">The SubjectNameStrategy</param>
        public static void Register(string name, ISubjectNameStrategy strategy)
        {
            map.Add(name, strategy);
        }
    }

    /// <summary>
    /// Common interface for subject name strategies.
    /// </summary>
    public interface ISubjectNameStrategy
    {
        /// <summary>
        /// Returns the schema registry value subject name.
        /// </summary>
        /// <param name="topic">The topic name.</param>
        /// <param name="isKey">If the message is a key or not.</param>
        /// <param name="schemaName">The fully qualified schema name.</param>
        /// <returns></returns>
        string ConstructSubjectName(string topic, bool isKey, string schemaName);
    }

    /// <summary>
    /// topic_name_strategy - The default. The subject is 'topic'-key or 'topic'-value.
    /// </summary>
    public class TopicSubjectNameStrategy : ISubjectNameStrategy
    {
        /// <inheritdoc />
        public string ConstructSubjectName(string topic, bool isKey, string schemaName)
        {
            string keyOrValue = isKey ? "key" : "value";
            return $"{topic}-{keyOrValue}";
        }
    }
    
    /// <summary>
    /// record_name_strategy - The subject is the fully qualified name of the schema.
    /// </summary>
    public class RecordSubjectNameStrategy : ISubjectNameStrategy
    {
        /// <inheritdoc />
        public string ConstructSubjectName(string topic, bool isKey, string schemaName)
        {
            return schemaName;
        }
    }
    
    /// <summary>
    /// topic_record_name_strategy - The subject is 'topic'-'the fully qualifed name of the schema'.
    /// </summary>
    public class TopicRecordSubjectNameStrategy : ISubjectNameStrategy
    {
        /// <inheritdoc />
        public string ConstructSubjectName(string topic, bool isKey, string schemaName)
        {
            return $"{topic}-{schemaName}";
        }
    }
}
