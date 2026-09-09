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
using System.Threading.Tasks;


namespace Confluent.SchemaRegistry.Encryption
{
    /// <summary>
    ///     An interface implemented by Confluent DEK Registry clients.
    /// </summary>
    public interface IDekRegistryClient : IDisposable
    {
        /// <summary>
        ///     The maximum capacity of the local cache.
        /// </summary>
        int MaxCachedKeys { get; }

        /// <summary>
        ///     Get the list of KEKs.
        /// </summary>
        /// <param name="ignoreDeletedKeks"></param>
        /// <returns></returns>
        public Task<List<string>> GetKeksAsync(bool ignoreDeletedKeks);

        /// <summary>
        ///     Get a KEK by name.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="ignoreDeletedKeks"></param>
        /// <returns></returns>
        public Task<RegisteredKek> GetKekAsync(string name, bool ignoreDeletedKeks);

        /// <summary>
        ///     Create a KEK.
        /// </summary>
        /// <param name="kek"></param>
        /// <returns></returns>
        public Task<RegisteredKek> CreateKekAsync(Kek kek);

        /// <summary>
        ///     Update a KEK.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="kek"></param>
        /// <returns></returns>
        public Task<RegisteredKek> UpdateKekAsync(string name, UpdateKek kek);

        /// <summary>
        ///     Get the list of DEKs.
        /// </summary>
        /// <param name="kekName"></param>
        /// <param name="ignoreDeletedDeks"></param>
        /// <returns></returns>
        public Task<List<string>> GetDeksAsync(string kekName, bool ignoreDeletedDeks);

        /// <summary>
        ///     Get the list of DEK versions.
        /// </summary>
        /// <param name="kekName"></param>
        /// <param name="subject"></param>
        /// <param name="algorithm"></param>
        /// <param name="ignoreDeletedDeks"></param>
        /// <returns></returns>
        public Task<List<int>> GetDekVersionsAsync(string kekName, string subject, DekFormat? algorithm,
            bool ignoreDeletedDeks);

        /// <summary>
        ///     Get a DEK.
        /// </summary>
        /// <param name="kekName"></param>
        /// <param name="subject"></param>
        /// <param name="algorithm"></param>
        /// <param name="ignoreDeletedDeks"></param>
        /// <returns></returns>
        public Task<RegisteredDek> GetDekAsync(string kekName, string subject, DekFormat? algorithm,
            bool ignoreDeletedDeks);

        /// <summary>
        ///     Get a DEK version.
        /// </summary>
        /// <param name="kekName"></param>
        /// <param name="subject"></param>
        /// <param name="version"></param>
        /// <param name="algorithm"></param>
        /// <param name="ignoreDeletedDeks"></param>
        /// <returns></returns>
        public Task<RegisteredDek> GetDekVersionAsync(string kekName, string subject, int version, DekFormat? algorithm,
            bool ignoreDeletedDeks);

        /// <summary>
        ///     Get a DEK latest version.
        /// </summary>
        /// <param name="kekName"></param>
        /// <param name="subject"></param>
        /// <param name="algorithm"></param>
        /// <param name="ignoreDeletedDeks"></param>
        /// <returns></returns>
        public Task<RegisteredDek> GetDekLatestVersionAsync(string kekName, string subject, DekFormat? algorithm,
            bool ignoreDeletedDeks);

        /// <summary>
        ///     Create a DEK.
        /// </summary>
        /// <param name="kekName"></param>
        /// <param name="kek"></param>
        /// <returns></returns>
        public Task<RegisteredDek> CreateDekAsync(string kekName, Dek dek);
    }
}