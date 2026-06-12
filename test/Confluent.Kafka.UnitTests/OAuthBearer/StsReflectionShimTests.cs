// Copyright 2026 Confluent Inc.
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
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;
using Confluent.Kafka.Internal.OAuthBearer.Aws;
using Moq;
using Xunit;

namespace Confluent.Kafka.UnitTests.OAuthBearer
{
    /// <summary>
    ///     R3 tests for <see cref="StsReflectionShim"/>. Two harnesses:
    ///     (a) real-AWS-SDK — the test project references AWSSDK.SecurityToken
    ///         3.7.504, exercising the production resolution path.
    ///     (b) synthetic-mismatch — assemblies that *don't* contain the expected
    ///         types are passed to <c>Bind</c> to verify the friendly errors fire.
    /// </summary>
    public class StsReflectionShimTests
    {
        // ---- Real-AWS-SDK harness ----

        [Fact]
        public void LoadOrThrow_RealAssembly_AllTargetsResolved()
        {
            var shim = StsReflectionShim.LoadOrThrow();

            Assert.NotNull(shim.StsAssembly);
            Assert.NotNull(shim.CoreAssembly);

            Assert.NotNull(shim.StsClientInterface);
            Assert.Equal("Amazon.SecurityToken.IAmazonSecurityTokenService",
                shim.StsClientInterface.FullName);

            Assert.NotNull(shim.StsClientType);
            Assert.NotNull(shim.StsClientConfigType);
            Assert.NotNull(shim.RequestType);
            Assert.NotNull(shim.ResponseType);
            Assert.NotNull(shim.RegionEndpointType);
            Assert.NotNull(shim.FallbackCredentialsFactoryType);

            Assert.NotNull(shim.GetWebIdentityTokenAsyncMethod);
            Assert.NotNull(shim.RegionEndpointGetBySystemName);
            Assert.NotNull(shim.FallbackCredentialsFactoryGetCredentials);

            Assert.NotNull(shim.RequestAudience);
            Assert.NotNull(shim.RequestSigningAlgorithm);
            Assert.NotNull(shim.RequestDurationSeconds);
            Assert.NotNull(shim.ResponseWebIdentityToken);
            Assert.NotNull(shim.ResponseExpiration);
            Assert.NotNull(shim.ConfigRegionEndpoint);
            Assert.NotNull(shim.ConfigServiceURL);
        }

        [Fact]
        public void LoadOrThrow_AssemblyVersion_AtLeastFloor()
        {
            var shim = StsReflectionShim.LoadOrThrow();
            Assert.True(shim.StsAssemblyVersion >= StsReflectionShim.MinAssemblyVersion,
                $"Loaded {StsReflectionShim.StsAssemblySimpleName} {shim.StsAssemblyVersion} " +
                $"is below floor {StsReflectionShim.MinAssemblyVersion}.");
        }

        [Fact]
        public void Bind_ResolvedRequestPropertiesMatchTypedShape()
        {
            var shim = StsReflectionShim.LoadOrThrow();

            // Audience must be List<string>; SigningAlgorithm string; DurationSeconds int? (or int).
            Assert.Equal(typeof(List<string>), shim.RequestAudience.PropertyType);
            Assert.Equal(typeof(string), shim.RequestSigningAlgorithm.PropertyType);
            Assert.True(shim.RequestDurationSeconds.PropertyType == typeof(int)
                     || shim.RequestDurationSeconds.PropertyType == typeof(int?),
                $"Unexpected DurationSeconds type {shim.RequestDurationSeconds.PropertyType}.");
        }

        [Fact]
        public void Bind_ResolvedResponsePropertiesMatchTypedShape()
        {
            var shim = StsReflectionShim.LoadOrThrow();

            Assert.Equal(typeof(string), shim.ResponseWebIdentityToken.PropertyType);
            Assert.Equal(typeof(DateTime), shim.ResponseExpiration.PropertyType);
        }

        [Fact]
        public void CreateStsClient_ValidRegion_ReturnsAmazonSecurityTokenServiceClient()
        {
            var shim = StsReflectionShim.LoadOrThrow();

            var client = shim.CreateStsClient("us-east-1", stsEndpointOverride: null);

            Assert.NotNull(client);
            Assert.IsAssignableFrom<IAmazonSecurityTokenService>(client);
        }

        [Fact]
        public void CreateStsClient_StsEndpointOverride_AppliedToServiceUrl()
        {
            var shim = StsReflectionShim.LoadOrThrow();

            var client = shim.CreateStsClient(
                "us-east-1",
                stsEndpointOverride: "https://sts-fips.us-east-1.amazonaws.com");

            Assert.NotNull(client);

            // Reflect the client's Config.ServiceURL to verify the override applied.
            var configProp = shim.StsClientType.GetProperty("Config",
                BindingFlags.Public | BindingFlags.Instance);
            var config = configProp.GetValue(client);
            var serviceUrl = (string)shim.ConfigServiceURL.GetValue(config);

            // AWS SDK normalises the URL by appending a trailing slash, so use StartsWith.
            Assert.StartsWith("https://sts-fips.us-east-1.amazonaws.com", serviceUrl);
        }

        [Fact]
        public void CreateStsClient_NullRegion_Throws()
        {
            var shim = StsReflectionShim.LoadOrThrow();

            Assert.Throws<ArgumentException>(
                () => shim.CreateStsClient(null, stsEndpointOverride: null));
        }

        [Fact]
        public async Task GetWebIdentityTokenAsync_MockClient_ReturnsTokenAndExpiration()
        {
            var shim = StsReflectionShim.LoadOrThrow();

            var fakeExpiration = new DateTime(2026, 4, 28, 12, 0, 0, DateTimeKind.Utc);
            var mockSts = new Mock<IAmazonSecurityTokenService>();
            mockSts
                .Setup(s => s.GetWebIdentityTokenAsync(
                    It.IsAny<GetWebIdentityTokenRequest>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(new GetWebIdentityTokenResponse
                {
                    WebIdentityToken = "fake.jwt.token",
                    Expiration = fakeExpiration,
                });

            var result = await shim.GetWebIdentityTokenAsync(
                stsClient: mockSts.Object,
                audience: "https://confluent.cloud/oidc",
                signingAlgorithm: "ES384",
                durationSeconds: 300,
                cancellationToken: CancellationToken.None);

            Assert.Equal("fake.jwt.token", result.Jwt);
            Assert.Equal(fakeExpiration, result.Expiration);
        }

        [Fact]
        public async Task GetWebIdentityTokenAsync_PropagatesRequestArgsToMock()
        {
            var shim = StsReflectionShim.LoadOrThrow();

            GetWebIdentityTokenRequest captured = null;
            var mockSts = new Mock<IAmazonSecurityTokenService>();
            mockSts
                .Setup(s => s.GetWebIdentityTokenAsync(
                    It.IsAny<GetWebIdentityTokenRequest>(),
                    It.IsAny<CancellationToken>()))
                .Callback<GetWebIdentityTokenRequest, CancellationToken>((req, _) => captured = req)
                .ReturnsAsync(new GetWebIdentityTokenResponse
                {
                    WebIdentityToken = "x.y.z",
                    Expiration = DateTime.UtcNow.AddMinutes(5),
                });

            await shim.GetWebIdentityTokenAsync(
                mockSts.Object,
                audience: "https://aud-test",
                signingAlgorithm: "RS256",
                durationSeconds: 1800,
                cancellationToken: CancellationToken.None);

            Assert.NotNull(captured);
            Assert.Single(captured.Audience);
            Assert.Equal("https://aud-test", captured.Audience[0]);
            Assert.Equal("RS256", captured.SigningAlgorithm);
            Assert.Equal(1800, captured.DurationSeconds);
        }

        [Fact]
        public async Task GetWebIdentityTokenAsync_StsThrows_PropagatesException()
        {
            var shim = StsReflectionShim.LoadOrThrow();

            var mockSts = new Mock<IAmazonSecurityTokenService>();
            mockSts
                .Setup(s => s.GetWebIdentityTokenAsync(
                    It.IsAny<GetWebIdentityTokenRequest>(),
                    It.IsAny<CancellationToken>()))
                .ThrowsAsync(new AmazonSecurityTokenServiceException("AccessDenied"));

            await Assert.ThrowsAsync<AmazonSecurityTokenServiceException>(() =>
                shim.GetWebIdentityTokenAsync(
                    mockSts.Object, "https://aud", "ES384", 300, CancellationToken.None));
        }

        // ---- Synthetic-mismatch harness ----

        [Fact]
        public void Bind_AssemblyMissingExpectedTypes_ThrowsFriendlyError()
        {
            // mscorlib / System.Private.CoreLib has none of the Amazon types.
            var nonAwsAssembly = typeof(string).Assembly;

            var ex = Assert.Throws<InvalidOperationException>(
                () => StsReflectionShim.Bind(
                    stsAssembly: nonAwsAssembly,
                    coreAssembly: nonAwsAssembly,
                    stsAssemblyVersion: StsReflectionShim.MinAssemblyVersion));

            Assert.Contains("Amazon.SecurityToken.IAmazonSecurityTokenService", ex.Message);
        }

        [Fact]
        public void Bind_VersionBelowFloor_ThrowsFriendlyError()
        {
            // Real assemblies (so type lookup would succeed if version check
            // allowed it), but pretend the version is one patch older than floor.
            var realStsAsm = typeof(IAmazonSecurityTokenService).Assembly;
            var realCoreAsm = typeof(Amazon.RegionEndpoint).Assembly;
            var oldVersion = new Version(3, 7, 503);

            var ex = Assert.Throws<InvalidOperationException>(
                () => StsReflectionShim.Bind(realStsAsm, realCoreAsm, oldVersion));

            Assert.Contains("3.7.503", ex.Message);
            Assert.Contains(StsReflectionShim.MinAssemblyVersion.ToString(), ex.Message);
            Assert.Contains("dotnet add package", ex.Message);
        }

        [Fact]
        public void Bind_VersionAtFloor_Accepted()
        {
            var realStsAsm = typeof(IAmazonSecurityTokenService).Assembly;
            var realCoreAsm = typeof(Amazon.RegionEndpoint).Assembly;
            var atFloor = StsReflectionShim.MinAssemblyVersion;

            // Should not throw.
            var shim = StsReflectionShim.Bind(realStsAsm, realCoreAsm, atFloor);
            Assert.NotNull(shim);
        }

        [Fact]
        public void Bind_NullStsAssembly_Throws()
        {
            Assert.Throws<ArgumentNullException>(
                () => StsReflectionShim.Bind(null, typeof(string).Assembly));
        }

        [Fact]
        public void Bind_NullCoreAssembly_Throws()
        {
            Assert.Throws<ArgumentNullException>(
                () => StsReflectionShim.Bind(typeof(IAmazonSecurityTokenService).Assembly, null));
        }
    }
}
