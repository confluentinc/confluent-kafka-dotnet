// Copyright 2025 Confluent Inc.
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

using System.Net.Http.Headers;
using System.Threading.Tasks;
namespace Confluent.SchemaRegistry
{
    /// <summary>
    ///     An interface defining HTTP client authentication header values.
    /// </summary>
    public interface IAuthenticationBearerHeaderValueProvider : IAuthenticationHeaderValueProvider
    {

        /// <summary>
        ///     Initializes or refreshes the authentication credentials.
        /// </summary>
        /// <returns>
        ///     A task representing the asynchronous initialization or refresh operation.
        /// </returns>
        public Task InitOrRefreshAsync();

        /// <inheritdoc/>
        public bool NeedsInitOrRefresh();

        /// <summary>
        ///   Get the logical cluster for HTTP requests
        /// </summary>
        /// <returns>
        ///   The logical cluster for HTTP request messages
        /// </returns>  
        string GetLogicalCluster();
        /// <summary>
        ///   Get the identity pool for HTTP requests
        /// </summary>
        /// <returns>
        ///   The identity pool for HTTP request messages
        /// </returns>
        string GetIdentityPool();
    }
}
