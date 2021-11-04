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

using System.Net.Http.Headers;

namespace Confluent.SchemaRegistry
{
    /// <summary>
    ///     A HTTP authenticator implementing the 'Bearer' scheme
    ///
    ///     See: https://datatracker.ietf.org/doc/html/rfc6750
    /// </summary>
    public class BearerHttpAuthenticator : IHttpAuthenticator
    {
        AuthenticationHeaderValue authenticationHeader;

        /// <inheritdoc/>
        public AuthenticationHeaderValue GetAuthenticationHeader() => authenticationHeader;

        /// <summary>
        ///     Set the OAuth bearer token
        /// </summary>
        /// <param name="token">
        ///     The new bearer token
        /// </param>
        public void SetBearerToken(string token)
        {
            authenticationHeader = new AuthenticationHeaderValue("Bearer", token);
        }
    }
}
