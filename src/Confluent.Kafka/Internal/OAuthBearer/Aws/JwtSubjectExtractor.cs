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
using System.Text.Json;

namespace Confluent.Kafka.Internal.OAuthBearer.Aws
{
    /// <summary>
    ///     Extracts the <c>sub</c> claim from an unverified JWT.
    /// </summary>
    /// <remarks>
    ///     Signature verification is deliberately out of scope — AWS STS signs the
    ///     token it returns from <c>GetWebIdentityToken</c>, and the relying-party
    ///     (Kafka broker) verifies the signature. The <c>sub</c> claim is used only
    ///     to populate <c>OAuthBearerToken.PrincipalName</c>, which is an
    ///     identification hint, not a security decision.
    /// </remarks>
    public static class JwtSubjectExtractor
    {
        // Live AWS-minted tokens are ~1.4 KB (librdkafka probe data).
        // 8 KB is a generous ceiling that prevents attacker-controlled allocation.
        private const int MaxTokenLengthChars = 8192;

        /// <summary>
        ///     Returns the value of the <c>sub</c> claim from the JWT's payload segment.
        /// </summary>
        /// <exception cref="FormatException">
        ///     Thrown if the input is null/empty, exceeds the size ceiling, has a wrong
        ///     number of segments, fails base64url decoding, is not valid JSON, or is
        ///     missing a non-empty <c>sub</c> claim.
        /// </exception>
        public static string ExtractSub(string jwt)
        {
            if (jwt == null)
            {
                throw new FormatException("JWT is null.");
            }
            if (jwt.Length == 0)
            {
                throw new FormatException("JWT is empty.");
            }
            if (jwt.Length > MaxTokenLengthChars)
            {
                throw new FormatException(
                    $"JWT length {jwt.Length} exceeds maximum allowed ({MaxTokenLengthChars}).");
            }

            var parts = jwt.Split('.');
            if (parts.Length != 3)
            {
                throw new FormatException(
                    $"JWT must have exactly 3 '.'-separated segments; got {parts.Length}.");
            }

            var payloadBytes = DecodeBase64UrlSegment(parts[1]);

            try
            {
                using var doc = JsonDocument.Parse(payloadBytes);
                if (doc.RootElement.ValueKind != JsonValueKind.Object)
                {
                    throw new FormatException("JWT payload is not a JSON object.");
                }
                if (!doc.RootElement.TryGetProperty("sub", out var subElement)
                    || subElement.ValueKind != JsonValueKind.String)
                {
                    throw new FormatException("JWT payload is missing a 'sub' string claim.");
                }

                var sub = subElement.GetString();
                if (string.IsNullOrEmpty(sub))
                {
                    throw new FormatException("JWT 'sub' claim is empty.");
                }
                return sub;
            }
            catch (JsonException ex)
            {
                throw new FormatException(
                    "JWT payload is not valid JSON: " + ex.Message, ex);
            }
        }

        private static byte[] DecodeBase64UrlSegment(string segment)
        {
            if (segment.Length == 0)
            {
                throw new FormatException("JWT payload segment is empty.");
            }

            var s = segment.Replace('-', '+').Replace('_', '/');
            switch (s.Length % 4)
            {
                case 0: break;
                case 2: s += "=="; break;
                case 3: s += "="; break;
                default:
                    throw new FormatException(
                        "JWT payload segment has invalid base64url length.");
            }

            try
            {
                return Convert.FromBase64String(s);
            }
            catch (FormatException ex)
            {
                throw new FormatException(
                    "JWT payload segment is not valid base64url: " + ex.Message, ex);
            }
        }
    }
}
