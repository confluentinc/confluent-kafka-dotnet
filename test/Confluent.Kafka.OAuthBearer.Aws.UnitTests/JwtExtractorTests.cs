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
using System.Text;
using Confluent.Kafka.OAuthBearer.Aws.Internal;
using Xunit;
using static Confluent.Kafka.OAuthBearer.Aws.UnitTests.AwsTestHelpers;

namespace Confluent.Kafka.OAuthBearer.Aws.UnitTests
{
    public class JwtExtractorTests
    {
        [Fact]
        public void ExtractSub_RoleArn_Returned()
        {
            var jwt = MakeJwt("{\"sub\":\"arn:aws:iam::123456789012:role/MyRole\",\"iat\":1}");
            Assert.Equal("arn:aws:iam::123456789012:role/MyRole",
                JwtExtractor.ExtractSub(jwt));
        }

        [Fact]
        public void ExtractSub_AssumedRoleArn_Returned()
        {
            var jwt = MakeJwt(
                "{\"sub\":\"arn:aws:sts::123456789012:assumed-role/MyRole/session-name\"}");
            Assert.Equal("arn:aws:sts::123456789012:assumed-role/MyRole/session-name",
                JwtExtractor.ExtractSub(jwt));
        }

        [Fact]
        public void ExtractSub_OtherClaimsIgnored()
        {
            var jwt = MakeJwt(
                "{\"iss\":\"https://x\",\"sub\":\"arn:aws:iam::1:role/R\"," +
                "\"aud\":\"a\",\"exp\":1,\"iat\":0,\"jti\":\"j\"}");
            Assert.Equal("arn:aws:iam::1:role/R", JwtExtractor.ExtractSub(jwt));
        }

        // Real STS payload (signature stripped — ExtractSub only reads parts[1]).
        private const string ExpectedRoleArn =
            "arn:aws:iam::708975691912:role/prashah-iam-sts-test-role";

        private const string RealEs384Jwt =
            "eyJraWQiOiJFQzM4NF8wIiwidHlwIjoiSldUIiwiYWxnIjoiRVMzODQifQ" +
            ".eyJhdWQiOiJodHRwczovL2FwaS5leGFtcGxlLmNvbSIsInN1YiI6ImFybjphd3M6aWFtOjo3MDg5NzU2OTE5MTI6cm9sZS9wcmFzaGFoLWlhbS1zdHMtdGVzdC1yb2xlIiwiaHR0cHM6Ly9zdHMuYW1hem9uYXdzLmNvbS8iOnsiZWMyX2luc3RhbmNlX3NvdXJjZV92cGMiOiJ2cGMtYWQ4NzMzYzQiLCJlYzJfcm9sZV9kZWxpdmVyeSI6IjIuMCIsIm9yZ19pZCI6Im8tMHgzdDh1bW9seiIsImF3c19hY2NvdW50IjoiNzA4OTc1NjkxOTEyIiwib3VfcGF0aCI6WyJvLTB4M3Q4dW1vbHovci16YzVqL291LXpjNWotNXJwd3pqbHIvIl0sIm9yaWdpbmFsX3Nlc3Npb25fZXhwIjoiMjAyNi0wNS0xNFQyMzoxNjozMloiLCJzb3VyY2VfcmVnaW9uIjoiZXUtbm9ydGgtMSIsImVjMl9zb3VyY2VfaW5zdGFuY2VfYXJuIjoiYXJuOmF3czplYzI6ZXUtbm9ydGgtMTo3MDg5NzU2OTE5MTI6aW5zdGFuY2UvaS0wOTc5MGY5OTY4YzExYTFjNyIsInByaW5jaXBhbF9pZCI6ImFybjphd3M6aWFtOjo3MDg5NzU2OTE5MTI6cm9sZS9wcmFzaGFoLWlhbS1zdHMtdGVzdC1yb2xlIiwicHJpbmNpcGFsX3RhZ3MiOnsiZGl2dnlfb3duZXIiOiJwcmFzaGFoQGNvbmZsdWVudC5pbyIsImRpdnZ5X2xhc3RfbW9kaWZpZWRfYnkiOiJwcmFzaGFoQGNvbmZsdWVudC5pbyJ9LCJlYzJfaW5zdGFuY2Vfc291cmNlX3ByaXZhdGVfaXB2NCI6IjE3Mi4zMS4zLjEwOSJ9LCJpc3MiOiJodHRwczovL2ExZWJjNzA1LWNkNGQtNDJiNC05M2I1LTk2ZTkzYWNmYjQzMS50b2tlbnMuc3RzLmdsb2JhbC5hcGkuYXdzIiwiZXhwIjoxNzc4Nzc4NzY2LCJpYXQiOjE3Nzg3Nzg0NjYsImp0aSI6IjFkM2QzZmMyLTBlNzktNDI2OS05NjcxLTJmODQ4NDYxOWZiNyJ9" +
            ".sig";

        private const string RealRs256Jwt =
            "eyJraWQiOiJSU0FfMCIsInR5cCI6IkpXVCIsImFsZyI6IlJTMjU2In0" +
            ".eyJhdWQiOiJodHRwczovL2FwaS5leGFtcGxlLmNvbSIsInN1YiI6ImFybjphd3M6aWFtOjo3MDg5NzU2OTE5MTI6cm9sZS9wcmFzaGFoLWlhbS1zdHMtdGVzdC1yb2xlIiwiaHR0cHM6Ly9zdHMuYW1hem9uYXdzLmNvbS8iOnsiZWMyX2luc3RhbmNlX3NvdXJjZV92cGMiOiJ2cGMtYWQ4NzMzYzQiLCJlYzJfcm9sZV9kZWxpdmVyeSI6IjIuMCIsIm9yZ19pZCI6Im8tMHgzdDh1bW9seiIsImF3c19hY2NvdW50IjoiNzA4OTc1NjkxOTEyIiwib3VfcGF0aCI6WyJvLTB4M3Q4dW1vbHovci16YzVqL291LXpjNWotNXJwd3pqbHIvIl0sIm9yaWdpbmFsX3Nlc3Npb25fZXhwIjoiMjAyNi0wNS0xNFQyMzoxNjozMloiLCJzb3VyY2VfcmVnaW9uIjoiZXUtbm9ydGgtMSIsImVjMl9zb3VyY2VfaW5zdGFuY2VfYXJuIjoiYXJuOmF3czplYzI6ZXUtbm9ydGgtMTo3MDg5NzU2OTE5MTI6aW5zdGFuY2UvaS0wOTc5MGY5OTY4YzExYTFjNyIsInByaW5jaXBhbF9pZCI6ImFybjphd3M6aWFtOjo3MDg5NzU2OTE5MTI6cm9sZS9wcmFzaGFoLWlhbS1zdHMtdGVzdC1yb2xlIiwicHJpbmNpcGFsX3RhZ3MiOnsiZGl2dnlfb3duZXIiOiJwcmFzaGFoQGNvbmZsdWVudC5pbyIsImRpdnZ5X2xhc3RfbW9kaWZpZWRfYnkiOiJwcmFzaGFoQGNvbmZsdWVudC5pbyJ9LCJlYzJfaW5zdGFuY2Vfc291cmNlX3ByaXZhdGVfaXB2NCI6IjE3Mi4zMS4zLjEwOSJ9LCJpc3MiOiJodHRwczovL2ExZWJjNzA1LWNkNGQtNDJiNC05M2I1LTk2ZTkzYWNmYjQzMS50b2tlbnMuc3RzLmdsb2JhbC5hcGkuYXdzIiwiZXhwIjoxNzc4Nzc4NzY2LCJpYXQiOjE3Nzg3Nzg0NjYsImp0aSI6IjU5OTliZDc1LTBmNTctNDMzMS1iOGExLWJkYzYwMjgxN2UyZiJ9" +
            ".sig";

        [Theory]
        [InlineData(RealEs384Jwt)]
        [InlineData(RealRs256Jwt)]
        public void ExtractSub_RealStsJwt_ReturnsExpectedArn(string jwt)
        {
            Assert.Equal(ExpectedRoleArn, JwtExtractor.ExtractSub(jwt));
        }

        [Fact]
        public void ExtractSub_UnpaddedBase64Url_Works()
        {
            var unpadded = MakeJwt("{\"sub\":\"a\"}");
            Assert.Equal("a", JwtExtractor.ExtractSub(unpadded));
        }

        [Fact]
        public void ExtractSub_PaddedBase64Url_AlsoWorks()
        {
            var header = Base64UrlEncode(Encoding.UTF8.GetBytes("{\"alg\":\"none\"}"));
            var payload = Convert.ToBase64String(Encoding.UTF8.GetBytes("{\"sub\":\"abc\"}"))
                .Replace('+', '-').Replace('/', '_'); // base64url but keep '='
            var jwt = $"{header}.{payload}.";
            Assert.Equal("abc", JwtExtractor.ExtractSub(jwt));
        }

        [Fact]
        public void ExtractSub_UrlSafeChars_Handled()
        {
            var bytes = Encoding.UTF8.GetBytes("{\"sub\":\"x\"}");
            var normal = Convert.ToBase64String(bytes);
            var urlSafe = normal.Replace('+', '-').Replace('/', '_').TrimEnd('=');
            var jwt = "aGVhZGVy." + urlSafe + ".c2ln";
            Assert.Equal("x", JwtExtractor.ExtractSub(jwt));
        }

        // Explicit per-branch coverage of the % 4 padding switch in
        // DecodeBase64UrlSegment.
        [Theory]
        [InlineData("{\"sub\":\"a\"}",   "a")]    // 11 bytes → trimmed 15 → %4 = 3 (case 3: one '=' added)
        [InlineData("{\"sub\":\"ab\"}",  "ab")]   // 12 bytes → trimmed 16 → %4 = 0 (case 0: no padding)
        [InlineData("{\"sub\":\"abc\"}", "abc")]  // 13 bytes → trimmed 18 → %4 = 2 (case 2: two '==' added)
        public void ExtractSub_PaddingBranchesAllHit_DecodesCorrectly(string payloadJson, string expectedSub)
        {
            var jwt = MakeJwt(payloadJson);
            Assert.Equal(expectedSub, JwtExtractor.ExtractSub(jwt));
        }

        [Fact]
        public void ExtractSub_Null_Throws()
        {
            var ex = Assert.Throws<FormatException>(() => JwtExtractor.ExtractSub(null));
            Assert.Contains("null", ex.Message);
        }

        [Fact]
        public void ExtractSub_Empty_Throws()
        {
            var ex = Assert.Throws<FormatException>(() => JwtExtractor.ExtractSub(""));
            Assert.Contains("empty", ex.Message);
            Assert.Contains("JWT is empty", ex.Message);
        }

        [Fact]
        public void ExtractSub_OneSegment_Throws()
        {
            var ex = Assert.Throws<FormatException>(
                () => JwtExtractor.ExtractSub("onlyonepart"));
            Assert.Contains("3", ex.Message);
        }

        [Fact]
        public void ExtractSub_TwoSegments_Throws()
        {
            var ex = Assert.Throws<FormatException>(
                () => JwtExtractor.ExtractSub("a.b"));
            Assert.Contains("3", ex.Message);
        }

        [Fact]
        public void ExtractSub_FourSegments_Throws()
        {
            var ex = Assert.Throws<FormatException>(
                () => JwtExtractor.ExtractSub("a.b.c.d"));
            Assert.Contains("3", ex.Message);
        }

        [Fact]
        public void ExtractSub_MalformedBase64InPayload_Throws()
        {
            var ex = Assert.Throws<FormatException>(
                () => JwtExtractor.ExtractSub("aGVhZGVy.not!base64.c2ln"));
            Assert.Contains("base64url", ex.Message);
        }

        [Fact]
        public void ExtractSub_MalformedJsonInPayload_Throws()
        {
            var badPayload = Base64UrlEncode(Encoding.UTF8.GetBytes("not json"));
            var ex = Assert.Throws<FormatException>(
                () => JwtExtractor.ExtractSub($"aGVhZGVy.{badPayload}.c2ln"));
            Assert.Contains("not valid JSON", ex.Message);
        }

        [Fact]
        public void ExtractSub_PayloadIsJsonArray_Throws()
        {
            var arrayPayload = Base64UrlEncode(Encoding.UTF8.GetBytes("[\"not\",\"an\",\"object\"]"));
            var ex = Assert.Throws<FormatException>(
                () => JwtExtractor.ExtractSub($"aGVhZGVy.{arrayPayload}.c2ln"));
            Assert.Contains("not a JSON object", ex.Message);
        }

        [Fact]
        public void ExtractSub_MissingSubClaim_Throws()
        {
            var jwt = MakeJwt("{\"iss\":\"https://x\",\"aud\":\"a\"}");
            var ex = Assert.Throws<FormatException>(() => JwtExtractor.ExtractSub(jwt));
            Assert.Contains("'sub'", ex.Message);
        }

        [Fact]
        public void ExtractSub_SubClaimIsNumber_Throws()
        {
            var jwt = MakeJwt("{\"sub\":12345}");
            var ex = Assert.Throws<FormatException>(() => JwtExtractor.ExtractSub(jwt));
            Assert.Contains("'sub'", ex.Message);
        }

        [Fact]
        public void ExtractSub_SubClaimIsNull_Throws()
        {
            var jwt = MakeJwt("{\"sub\":null}");
            var ex = Assert.Throws<FormatException>(() => JwtExtractor.ExtractSub(jwt));
            Assert.Contains("'sub'", ex.Message);
        }

        [Fact]
        public void ExtractSub_SubClaimIsEmptyString_Throws()
        {
            var jwt = MakeJwt("{\"sub\":\"\"}");
            var ex = Assert.Throws<FormatException>(() => JwtExtractor.ExtractSub(jwt));
            Assert.Contains("empty", ex.Message);
            Assert.Contains("'sub'", ex.Message);
        }

        [Fact]
        public void ExtractSub_OversizedInput_Throws()
        {
            var oversized = new string('a', 8193);
            var ex = Assert.Throws<FormatException>(
                () => JwtExtractor.ExtractSub(oversized));
            Assert.Contains("exceeds maximum", ex.Message);
        }

        [Fact]
        public void ExtractSub_AtCeiling_ReachesParser()
        {
            var atCeiling = new string('a', 8192);
            var ex = Assert.Throws<FormatException>(
                () => JwtExtractor.ExtractSub(atCeiling));
            Assert.Contains("3", ex.Message); // segment-count error, not ceiling error
        }

        [Fact]
        public void ExtractSub_EmptyPayloadSegment_Throws()
        {
            var ex = Assert.Throws<FormatException>(
                () => JwtExtractor.ExtractSub("header..sig"));
            Assert.Contains("empty", ex.Message);
            Assert.Contains("segment", ex.Message);
        }

    }
}
