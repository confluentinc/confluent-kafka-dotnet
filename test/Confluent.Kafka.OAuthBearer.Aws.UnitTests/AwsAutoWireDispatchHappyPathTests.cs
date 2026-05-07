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
using System.Collections.Generic;
using Confluent.Kafka.Internal.OAuthBearer.Aws;
using Xunit;

namespace Confluent.Kafka.OAuthBearer.Aws.UnitTests
{
    /// <summary>
    ///     Cross-tests <see cref="AwsAutoWireDispatcher"/> (core) against
    ///     <see cref="AwsAutoWire"/> (optional pkg). Lives in the optional-pkg
    ///     test project because it requires both assemblies to be loaded —
    ///     <c>Confluent.Kafka.UnitTests</c> deliberately doesn't reference the
    ///     optional package, so it can't exercise the dispatcher's happy path.
    /// </summary>
    public class AwsAutoWireDispatchHappyPathTests
    {
        [Fact]
        public void LoadHandler_OptionalPackagePresent_ReturnsHandler()
        {
            AwsAutoWireDispatcher.ResetCacheForTests();
            var snap = new Dictionary<string, string>
            {
                ["sasl.oauthbearer.metadata.authentication.type"] = "aws_iam",
                ["sasl.oauthbearer.config"] = "region=us-east-1 audience=https://a",
            };
            var handler = AwsAutoWireDispatcher.LoadHandler(snap);
            Assert.NotNull(handler);
        }

        [Fact]
        public void LoadHandler_ParserError_UnwrappedFromTargetInvocationException()
        {
            // The optional pkg's CreateHandler throws ArgumentException for missing
            // 'region'. Reflection wraps it in TargetInvocationException, which
            // AwsAutoWireDispatcher must unwrap via ExceptionDispatchInfo so callers
            // see the original ArgumentException with its original stack trace.
            AwsAutoWireDispatcher.ResetCacheForTests();
            var snap = new Dictionary<string, string>
            {
                ["sasl.oauthbearer.metadata.authentication.type"] = "aws_iam",
                ["sasl.oauthbearer.config"] = "audience=https://a",   // missing region
            };

            var ex = Assert.Throws<ArgumentException>(
                () => AwsAutoWireDispatcher.LoadHandler(snap));
            Assert.Contains("region", ex.Message);
        }
    }
}
