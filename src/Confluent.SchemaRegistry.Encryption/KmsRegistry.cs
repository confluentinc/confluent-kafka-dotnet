// Copyright 2024 Confluent Inc.
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

using System.Collections.Generic;
using System.Threading;

namespace Confluent.SchemaRegistry.Encryption
{
    /// <summary>
    ///     A KMS driver and client registry.
    /// </summary>
    public static class KmsRegistry
    {
        private static readonly SemaphoreSlim kmsDriversMutex = new SemaphoreSlim(1);
        private static readonly SemaphoreSlim kmsClientsMutex = new SemaphoreSlim(1);

        private static IList<IKmsDriver> kmsDrivers = new List<IKmsDriver>();
        private static IList<IKmsClient> kmsClients = new List<IKmsClient>();
        
        public static void RegisterKmsDriver(IKmsDriver kmsDriver)
        {
            kmsDriversMutex.Wait();
            try
            {
                kmsDrivers.Add(kmsDriver);
            }
            finally
            {
                kmsDriversMutex.Release();
            }
        }
        
        public static IKmsDriver GetKmsDriver(string keyUrl)
        {
            kmsDriversMutex.Wait();
            try
            {
                foreach (var kmsDriver in kmsDrivers)
                {
                    if (keyUrl.StartsWith(kmsDriver.GetKeyUrlPrefix()))
                    {
                        return kmsDriver;
                    }
                }
            }
            finally
            {
                kmsDriversMutex.Release();
            }

            return null;
        }
        
        public static void RegisterKmsClient(IKmsClient kmsClient)
        {
            kmsClientsMutex.Wait();
            try
            {
                kmsClients.Add(kmsClient);
            }
            finally
            {
                kmsClientsMutex.Release();
            }
        }
        
        public static IKmsClient GetKmsClient(string keyUrl)
        {
            kmsClientsMutex.Wait();
            try
            {
                foreach (var kmsClient in kmsClients)
                {
                    if (kmsClient.DoesSupport(keyUrl))
                    {
                        return kmsClient;
                    }
                }
            }
            finally
            {
                kmsClientsMutex.Release();
            }

            return null;
        }
    }
}
