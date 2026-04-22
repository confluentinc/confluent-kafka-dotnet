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
    ///     A hand-written test double that subclasses
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
