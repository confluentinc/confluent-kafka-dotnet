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

using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Xunit;

namespace Confluent.SchemaRegistry.UnitTests
{
    public class RestServiceTests
    {
        [Fact]
        public async Task HttpRequestException_PreservesInnerException_SingleUrl()
        {
            // Use a non-routable address to trigger HttpRequestException.
            var restService = new RestService(
                "http://localhost:1",
                5000,
                null,
                new List<X509Certificate2>(),
                true);

            var ex = await Assert.ThrowsAsync<HttpRequestException>(
                () => restService.GetSubjectsAsync());

            Assert.NotNull(ex.InnerException);
        }

        [Fact]
        public async Task HttpRequestException_PreservesInnerExceptions_MultipleUrls()
        {
            // Use multiple non-routable addresses to trigger AggregateException.
            var restService = new RestService(
                "http://localhost:1,http://localhost:2",
                5000,
                null,
                new List<X509Certificate2>(),
                true);

            var ex = await Assert.ThrowsAsync<HttpRequestException>(
                () => restService.GetSubjectsAsync());

            Assert.NotNull(ex.InnerException);
            Assert.IsType<AggregateException>(ex.InnerException);

            var aggEx = (AggregateException)ex.InnerException;
            Assert.Equal(2, aggEx.InnerExceptions.Count);
        }

        [Fact]
        public async Task Timeout_IsTreatedAsNetworkError_AndDoesNotPropagateRaw()
        {
            // A TCP listener that accepts connections but never sends a response
            // forces the HttpClient request to time out. Request timeouts surface
            // as TaskCanceledException / OperationCanceledException, which used to
            // bypass the multi-URL failover logic and propagate directly (see
            // confluent-kafka-dotnet issue #2626). They should now be treated like
            // a network error and converted into an HttpRequestException.
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            var port = ((IPEndPoint)listener.LocalEndpoint).Port;

            // Accept connections and hold them open without ever responding.
            var accepted = new List<TcpClient>();
            var acceptLoop = Task.Run(async () =>
            {
                try
                {
                    while (true)
                    {
                        accepted.Add(await listener.AcceptTcpClientAsync());
                    }
                }
                catch
                {
                    // listener stopped - expected on cleanup.
                }
            });

            try
            {
                var restService = new RestService(
                    $"http://localhost:{port}",
                    500, // short request timeout
                    null,
                    new List<X509Certificate2>(),
                    true,
                    maxRetries: 1,
                    retriesWaitMs: 1,
                    retriesMaxWaitMs: 2);

                var ex = await Assert.ThrowsAsync<HttpRequestException>(
                    () => restService.GetSubjectsAsync());

                Assert.NotNull(ex.InnerException);
            }
            finally
            {
                listener.Stop();
                foreach (var client in accepted)
                {
                    client.Dispose();
                }
            }
        }
    }
}
