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

namespace Confluent.Kafka.UnitTests.OAuthBearer
{
    /// <summary>
    ///     Tests for <see cref="AwsAutoWireDispatcher.LoadHandler"/> — the
    ///     core-side reflection bridge into the optional package. The
    ///     <c>Confluent.Kafka.UnitTests</c> project deliberately does <b>not</b>
    ///     reference <c>Confluent.Kafka.OAuthBearer.Aws</c> — exercising the
    ///     "pkg missing" failure mode requires the optional package to be
    ///     absent from the test bin/.
    /// </summary>
    /// <remarks>
    ///     Happy-path tests (pkg present) live in the optional-pkg test
    ///     project: <c>AwsAutoWireDispatcherTests</c> at
    ///     <c>test/Confluent.Kafka.OAuthBearer.Aws.UnitTests/</c>.
    /// </remarks>
    public class AwsAutoWireDispatcherTests
    {
        [Fact]
        public void LoadHandler_NullConfig_Throws()
        {
            // Reset the dispatcher's MethodInfo cache to ensure a fresh resolve attempt.
            AwsAutoWireDispatcher.ResetCacheForTests();
            Assert.Throws<ArgumentNullException>(() => AwsAutoWireDispatcher.LoadHandler(null));
        }

        [Fact]
        public void LoadHandler_OptionalPackageMissing_ThrowsFriendlyInvalidOperation()
        {
            // The Confluent.Kafka.UnitTests project does not reference
            // Confluent.Kafka.OAuthBearer.Aws — its assembly is therefore not
            // resolvable. The dispatcher should translate the underlying
            // FileNotFoundException into a friendly InvalidOperationException
            // pointing the user at the missing PackageReference.
            AwsAutoWireDispatcher.ResetCacheForTests();
            var snap = new Dictionary<string, string>
            {
                ["sasl.oauthbearer.metadata.authentication.type"] = "aws_iam",
                ["sasl.oauthbearer.method"] = "oidc",
                ["sasl.oauthbearer.config"] = "region=us-east-1 audience=https://a",
            };
            var ex = Assert.Throws<InvalidOperationException>(
                () => AwsAutoWireDispatcher.LoadHandler(snap));
            Assert.Contains("Confluent.Kafka.OAuthBearer.Aws", ex.Message);
            Assert.Contains("PackageReference", ex.Message);
            // Inner exception preserved for diagnostics.
            Assert.NotNull(ex.InnerException);
        }
    }
}
