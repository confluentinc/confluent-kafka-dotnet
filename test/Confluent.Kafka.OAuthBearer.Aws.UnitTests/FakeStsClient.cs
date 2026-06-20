// Copyright 2026 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.Runtime;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;

namespace Confluent.Kafka.OAuthBearer.Aws.UnitTests
{
    /// <summary>
    ///     Hand-written test double that subclasses
    ///     <see cref="AmazonSecurityTokenServiceClient"/> and overrides only
    ///     <see cref="GetWebIdentityTokenAsync(GetWebIdentityTokenRequest, CancellationToken)"/>.
    ///     Avoids adding Moq as a test-time dependency.
    /// </summary>
    internal sealed class FakeStsClient : AmazonSecurityTokenServiceClient
    {
        public GetWebIdentityTokenRequest LastRequest { get; private set; }
        public int CallCount { get; private set; }

        private readonly Func<GetWebIdentityTokenRequest, CancellationToken, Task<GetWebIdentityTokenResponse>> _responder;

        public FakeStsClient(
            Func<GetWebIdentityTokenRequest, CancellationToken, Task<GetWebIdentityTokenResponse>> responder)
            // Explicit credentials + region keep the base ctor off the network.
            : base(new BasicAWSCredentials("fake", "fake"),
                   new AmazonSecurityTokenServiceConfig { RegionEndpoint = RegionEndpoint.USEast1 })
        {
            _responder = responder;
        }

        public override Task<GetWebIdentityTokenResponse> GetWebIdentityTokenAsync(
            GetWebIdentityTokenRequest request,
            CancellationToken cancellationToken = default)
        {
            LastRequest = request;
            CallCount++;
            return _responder(request, cancellationToken);
        }
    }
}
