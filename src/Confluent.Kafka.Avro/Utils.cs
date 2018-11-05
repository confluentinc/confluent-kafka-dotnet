// Copyright 2018 Confluent Inc.
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
using System.Linq;


namespace Confluent.Kafka.AvroSerdes
{
    internal static class Utils
    {
        public static object ExtractPropertyValue(IEnumerable<KeyValuePair<string, string>> config, string name, string className, Type type)
        {
            var properties = config.Where(ci => ci.Key == name);

            if (properties.Count() > 1)
            {
                throw new ArgumentException($"{className} {name} configuration parameter was specified more than once.");
            }

            if (properties.Count() == 1)
            {
                try
                {
                    if (type == typeof(bool))
                    {
                        return bool.Parse(properties.First().Value);
                    }
                    else if (type == typeof(int))
                    {
                        return int.Parse(properties.First().Value);
                    }
                    throw new NotImplementedException($"cannot parse property of type {type.ToString()}");
                }
                catch (Exception e)
                {
                    throw new ArgumentException($"{className} {name} configuration parameter was incorrectly specified.", e);
                }
            }

            return null;
        }
    }
}
