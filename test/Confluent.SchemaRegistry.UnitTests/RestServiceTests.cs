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
using System.Net.Http;
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
    }
}
