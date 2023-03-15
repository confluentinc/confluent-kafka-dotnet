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
using System.IO;
using System.Reflection;
using Newtonsoft.Json.Linq;


namespace Confluent.SchemaRegistry.IntegrationTests
{
    public static partial class Tests
    {
        private static List<object[]> schemaRegistryParameters;
        static Tests()
        {
            // Quick fix for https://github.com/Microsoft/vstest/issues/918
            // Some tests will log using ConsoleLogger which print to standard Err by default, bugged on vstest
            // If we have error in test, they may hang
            // Write to standard output solve the issue
            Console.SetError(Console.Out);
        }

        public static IEnumerable<object[]> SchemaRegistryParameters()
        {
            if (schemaRegistryParameters == null)
            {
                var assemblyPath = typeof(Tests).GetTypeInfo().Assembly.Location;
                var assemblyDirectory = Path.GetDirectoryName(assemblyPath);
                var jsonPath = Path.Combine(assemblyDirectory, "schema.registry.parameters.json");
                var json = JObject.Parse(File.ReadAllText(jsonPath));
                var config = new Config();
                config.Server = json["server"].ToString();
                config.ServerWithAuth = json["server_with_auth"].ToString();
                config.ServerWithSsl = json["server_with_ssl"].ToString();
                config.Username = json["username"].ToString();
                config.Password = json["password"].ToString();
                config.KeystoreLocation = json["keystore_location"].ToString();
                config.KeystorePassword = json["keystore_password"].ToString();
                config.CaLocation = json["ca_location"].ToString();
                config.EnableSslCertificateVerification = json["enable_ssl_certificate_verification"].ToString();
                schemaRegistryParameters = new List<object[]> { new object[] { config } };
            }
            return schemaRegistryParameters;
        }
        public static bool semaphoreSkipFlakyTests(){
            string onSemaphore = Environment.GetEnvironmentVariable("SEMAPHORE_SKIP_FLAKY_TETSTS");
            if (onSemaphore != null)
            {
                return true;
            }
            return false;
        }
    }
}
