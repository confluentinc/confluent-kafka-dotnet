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
using System.Text;
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
        public async Task RetriableStatus_SurfacesAsSchemaRegistryException_WithStatusAndErrorCode()
        {
            // A retriable error response used to be reported as a plain
            // HttpRequestException once all URLs were exhausted, with the status
            // and error code available only as text in the message. Callers that
            // switch on SchemaRegistryException.Status (e.g. the CSFLE executor
            // treating 404 as "no dek") therefore had a hole for these statuses.
            using (var server = new StubHttpServer(
                       HttpStatusCode.ServiceUnavailable,
                       "{\"error_code\":50070,\"message\":\"Key 'test-value' not found\"}"))
            {
                var restService = new RestService(
                    server.Url,
                    5000,
                    null,
                    new List<X509Certificate2>(),
                    true,
                    maxRetries: 1,
                    retriesWaitMs: 1,
                    retriesMaxWaitMs: 2);

                var ex = await Assert.ThrowsAsync<SchemaRegistryException>(
                    () => restService.GetSubjectsAsync());

                Assert.Equal(HttpStatusCode.ServiceUnavailable, ex.Status);
                Assert.Equal(50070, ex.ErrorCode);
                Assert.Contains("Key 'test-value' not found", ex.Message);
            }
        }

        [Fact]
        public async Task RetriableStatus_WithUnparseableBody_PreservesStatus()
        {
            // The status must survive even when the error body is not in the
            // Schema Registry format (e.g. an HTML response from a proxy).
            using (var server = new StubHttpServer(
                       HttpStatusCode.InternalServerError,
                       "<html>gateway error</html>"))
            {
                var restService = new RestService(
                    server.Url,
                    5000,
                    null,
                    new List<X509Certificate2>(),
                    true,
                    maxRetries: 1,
                    retriesWaitMs: 1,
                    retriesMaxWaitMs: 2);

                var ex = await Assert.ThrowsAsync<SchemaRegistryException>(
                    () => restService.GetSubjectsAsync());

                Assert.Equal(HttpStatusCode.InternalServerError, ex.Status);
                Assert.Equal(-1, ex.ErrorCode);
            }
        }

        [Fact]
        public async Task NonRetriableStatus_SurfacesAsSchemaRegistryException()
        {
            using (var server = new StubHttpServer(
                       HttpStatusCode.NotFound,
                       "{\"error_code\":40470,\"message\":\"Key 'test-value' not found\"}"))
            {
                var restService = new RestService(
                    server.Url,
                    5000,
                    null,
                    new List<X509Certificate2>(),
                    true,
                    maxRetries: 1,
                    retriesWaitMs: 1,
                    retriesMaxWaitMs: 2);

                var ex = await Assert.ThrowsAsync<SchemaRegistryException>(
                    () => restService.GetSubjectsAsync());

                Assert.Equal(HttpStatusCode.NotFound, ex.Status);
                Assert.Equal(40470, ex.ErrorCode);
            }
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
                        accepted.Add(await listener.AcceptTcpClientAsync(TestContext.Current.CancellationToken));
                    }
                }
                catch
                {
                    // listener stopped - expected on cleanup.
                }
            }, TestContext.Current.CancellationToken);

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
                // Stop the listener and let the accept loop finish before
                // enumerating the accepted clients, to avoid mutating the list
                // while it is being read.
                listener.Stop();
                await acceptLoop;
                foreach (var client in accepted)
                {
                    client.Dispose();
                }
            }
        }

        /// <summary>
        ///     An HTTP server that answers every request with the same status and body.
        /// </summary>
        private class StubHttpServer : IDisposable
        {
            private readonly TcpListener listener;
            private readonly Task listenLoop;

            public string Url { get; }

            public StubHttpServer(HttpStatusCode status, string body)
            {
                // A raw socket rather than an HttpListener, which on Windows can
                // require a URL reservation to start.
                listener = new TcpListener(IPAddress.Loopback, 0);
                listener.Start();
                Url = $"http://localhost:{((IPEndPoint)listener.LocalEndpoint).Port}";

                var bodyBytes = Encoding.UTF8.GetBytes(body);
                var headerBytes = Encoding.ASCII.GetBytes(
                    $"HTTP/1.1 {(int)status} {status}\r\n" +
                    "Content-Type: application/vnd.schemaregistry.v1+json\r\n" +
                    $"Content-Length: {bodyBytes.Length}\r\n" +
                    "Connection: close\r\n\r\n");

                listenLoop = Task.Run(async () =>
                {
                    try
                    {
                        while (true)
                        {
                            using var client = await listener.AcceptTcpClientAsync();
                            var stream = client.GetStream();
                            await ReadRequestAsync(stream);
                            await stream.WriteAsync(headerBytes, 0, headerBytes.Length);
                            await stream.WriteAsync(bodyBytes, 0, bodyBytes.Length);
                            await stream.FlushAsync();
                        }
                    }
                    catch
                    {
                        // listener stopped - expected on cleanup.
                    }
                });
            }

            /// <summary>
            ///     Consume the request up to the end of its headers, so that the
            ///     client isn't answered before it has finished sending.
            /// </summary>
            private static async Task ReadRequestAsync(NetworkStream stream)
            {
                var buffer = new byte[1024];
                var request = new StringBuilder();
                while (request.ToString().IndexOf("\r\n\r\n", StringComparison.Ordinal) < 0)
                {
                    var read = await stream.ReadAsync(buffer, 0, buffer.Length);
                    if (read == 0)
                    {
                        return;
                    }

                    request.Append(Encoding.ASCII.GetString(buffer, 0, read));
                }
            }

            public void Dispose()
            {
                listener.Stop();
                listenLoop.Wait();
            }
        }
    }
}
