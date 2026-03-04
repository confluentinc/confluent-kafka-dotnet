using System;
using System.Data.Common;
using System.Diagnostics.Contracts;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Generic;
using Xunit;
using Confluent.SchemaRegistry;

namespace Confluent.SchemaRegistry.UnitTests.Rest
{
    public class LocalTests
    {
        private int maxRetries = 3;
        private int retriesWaitMs = 1000;
        private int retriesMaxWaitMs = 5000;
        private string clientId = "0oa7xtb1qirnWKZuB4x7";
        private string clientSecret = "7NKqv0YgYvrCZOx2LweMLeMwI9o9TsFZuWJZCxtc";
        private string scope = "schema_registry";
        private string tokenEndpoint = "https://dev-531534.okta.com/oauth2/ausmmrzyxaR6yZ1qe4x7/v1/token";
        private string logicalCluster = "lsrc-q6m5op";
        private string identityPool = "pool-GexQ";
        private string schemaRegistryUrl = "https://psrc-1ymy5nj.us-east-1.aws.confluent.cloud";
        [Fact]
        public async Task LocalTest()
        {
            // Arrange
            var provider = new BearerAuthenticationHeaderValueProvider(new HttpClient(), clientId, clientSecret, scope, tokenEndpoint, logicalCluster, identityPool, maxRetries, retriesWaitMs, retriesMaxWaitMs);

            await provider.InitOrRefreshAsync();

            // Act
            var header = provider.GetAuthenticationHeader();
            Console.WriteLine(header.Parameter);

            // Assert
            Assert.NotNull(header);
            Assert.Equal("Bearer", header.Scheme);
            Assert.NotEmpty(header.Parameter);

            var schemaRegistryConfig = new SchemaRegistryConfig
            {   
                Url = schemaRegistryUrl,
                BearerAuthCredentialsSource = BearerAuthCredentialsSource.OAuthBearer,
                BearerAuthClientId = clientId,
                BearerAuthClientSecret = clientSecret,
                BearerAuthScope = scope,
                BearerAuthTokenEndpointUrl = tokenEndpoint,
                BearerAuthLogicalCluster = logicalCluster,
                BearerAuthIdentityPoolId = identityPool
            };

            var schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryConfig);

            var schema = await schemaRegistryClient.GetAllSubjectsAsync();
            // Console.WriteLine("FUCK ME " + string.Join(", ", schema));

            // var schemaRegistryConfig2 = new SchemaRegistryConfig
            // {
            //     Url = "https://psrc-12dd33.us-west2.gcp.confluent.cloud",
            //     BasicAuthUserInfo = "WGAFPJAEV6WLUE4W:bUGfGrJDVinEVGpaYrCt9mNgCpMCRw0cnE4tjeiW06t1nHuDHkbldjMs9l5YsaWS"
            // };

            // var srClient2 = new CachedSchemaRegistryClient(schemaRegistryConfig2);
            // var schema2 = await srClient2.GetAllSubjectsAsync();

            // var schemaRegistryConfig3 = new SchemaRegistryConfig
            // {
            //     Url = "https://psrc-12dd33.us-west2.gcp.confluent.cloud",
            //     BasicAuthCredentialsSource = AuthCredentialsSource.UserInfo,
            //     BasicAuthUserInfo = "WGAFPJAEV6WLUE4W:bUGfGrJDVinEVGpaYrCt9mNgCpMCRw0cnE4tjeiW06t1nHuDHkbldjMs9l5YsaWS"
            // };

            // var srClient3 = new CachedSchemaRegistryClient(schemaRegistryConfig3);
            // var schema3 = await srClient3.GetAllSubjectsAsync();
            // Console.WriteLine("FUCK ME3 " + string.Join(", ", schema3));
        }
    }
}