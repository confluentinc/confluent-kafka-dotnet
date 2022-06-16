// Copyright 2016-2017 Confluent Inc.
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
using System.IO;
using System.Reflection;
using Newtonsoft.Json.Linq;


namespace Confluent.SchemaRegistry.Serdes.IntegrationTests
{
    public static partial class Tests
    {
        private static List<object[]> testParameters;

        static Tests()
        {
            // Quick fix for https://github.com/Microsoft/vstest/issues/918
            // Some tests will log using ConsoleLogger which print to standard Err by default, bugged on vstest
            // If we have error in test, they may hang
            // Write to standard output solve the issue
            Console.SetError(Console.Out);
        }

        public static IEnumerable<object[]> TestParameters()
        {
            if (testParameters == null)
            {
                var codeBasePath = Assembly.GetExecutingAssembly().Location;
                var dirPath = Path.GetDirectoryName(codeBasePath);
                var jsonPath = Path.Combine(dirPath, "testconf.json");

                var json = JObject.Parse(File.ReadAllText(jsonPath));
                testParameters = new List<object[]>
                {
                    new object[]
                    {
                        json["bootstrapServer"].ToString(),
                        json["schemaRegistryServer"].ToString()
                    }
                };
            }
            return testParameters;
        }
    }
}
