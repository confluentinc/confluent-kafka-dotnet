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
using System.Text;
using Confluent.Kafka.OAuthBearer.Aws.Internal;

namespace Confluent.Kafka.OAuthBearer.Aws.UnitTests
{
    /// <summary>
    ///     Shared test helpers for the AWS OAUTHBEARER plugin tests.
    ///     Consolidates <c>MakeJwt</c> + <c>Base64UrlEncode</c> (previously duplicated
    ///     across three test files) and the <c>RecordingSink</c> double (previously
    ///     duplicated across two).
    /// </summary>
    internal static class AwsTestHelpers
    {
        /// <summary>
        ///     Builds a 3-segment JWT with the given payload JSON. Header and
        ///     signature segments are placeholders — AwsJwtSubjectExtractor only
        ///     decodes the payload (<c>parts[1]</c>), so their content is unused
        ///     in tests.
        /// </summary>
        public static string MakeJwt(string payloadJson, string alg = "ES384")
        {
            var header = Base64UrlEncode(
                Encoding.UTF8.GetBytes($"{{\"alg\":\"{alg}\",\"typ\":\"JWT\"}}"));
            var payload = Base64UrlEncode(Encoding.UTF8.GetBytes(payloadJson));
            return $"{header}.{payload}.sig";
        }

        /// <summary>
        ///     Base64url-encodes a byte array: standard base64, trim '=' padding,
        ///     swap '+' → '-' and '/' → '_'.
        /// </summary>
        public static string Base64UrlEncode(byte[] bytes)
            => Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');
    }

    /// <summary>
    ///     <see cref="ITokenSink"/> test double — records every <c>SetToken</c> /
    ///     <c>SetTokenFailure</c> call so tests can assert on what was sent
    ///     without a real librdkafka client.
    /// </summary>
    internal sealed class RecordingSink : ITokenSink
    {
        public readonly List<(string TokenValue, long LifetimeMs, string PrincipalName,
                              IDictionary<string, string> Extensions)> SetCalls
            = new List<(string, long, string, IDictionary<string, string>)>();

        public readonly List<string> FailureCalls = new List<string>();

        public void SetToken(string tokenValue, long lifetimeMs, string principalName,
            IDictionary<string, string> extensions)
            => SetCalls.Add((tokenValue, lifetimeMs, principalName, extensions));

        public void SetTokenFailure(string error) => FailureCalls.Add(error);
    }
}
