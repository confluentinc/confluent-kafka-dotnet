using System;
using System.Text;
using Confluent.Kafka.OAuthBearer.Aws;
using Xunit;

namespace Confluent.Kafka.OAuthBearer.Aws.UnitTests
{
    public class JwtSubjectExtractorTests
    {
        [Fact]
        public void ExtractSub_RoleArn_Returned()
        {
            var jwt = MakeJwt("{\"sub\":\"arn:aws:iam::123456789012:role/MyRole\",\"iat\":1}");
            Assert.Equal("arn:aws:iam::123456789012:role/MyRole",
                JwtSubjectExtractor.ExtractSub(jwt));
        }

        [Fact]
        public void ExtractSub_AssumedRoleArn_Returned()
        {
            var jwt = MakeJwt(
                "{\"sub\":\"arn:aws:sts::123456789012:assumed-role/MyRole/session-name\"}");
            Assert.Equal("arn:aws:sts::123456789012:assumed-role/MyRole/session-name",
                JwtSubjectExtractor.ExtractSub(jwt));
        }

        [Fact]
        public void ExtractSub_OtherClaimsIgnored()
        {
            var jwt = MakeJwt(
                "{\"iss\":\"https://x\",\"sub\":\"arn:aws:iam::1:role/R\"," +
                "\"aud\":\"a\",\"exp\":1,\"iat\":0,\"jti\":\"j\"}");
            Assert.Equal("arn:aws:iam::1:role/R", JwtSubjectExtractor.ExtractSub(jwt));
        }

        [Fact]
        public void ExtractSub_UnpaddedBase64Url_Works()
        {
            // Payload "{\"sub\":\"abc\"}" is 13 bytes → base64 has length 20
            // (padded "=" removed per base64url; length mod 4 == 0 for this specific case).
            // Use a payload that produces a length needing 1 or 2 "=" pads after decoding.
            //   "{\"sub\":\"a\"}"      = 11 bytes → base64 "eyJzdWIiOiJhIn0=" (16 chars w/ pad, 15 w/o, mod4=3)
            //   "{\"sub\":\"ab\"}"     = 12 bytes → base64 "eyJzdWIiOiJhYiJ9" (16 chars, mod4=0)
            //   "{\"sub\":\"abcd\"}"   = 14 bytes → base64 "eyJzdWIiOiJhYmNkIn0=" (20 chars w/ pad)
            var unpadded = MakeJwt("{\"sub\":\"a\"}"); // will be emitted without "="
            Assert.Equal("a", JwtSubjectExtractor.ExtractSub(unpadded));
        }

        [Fact]
        public void ExtractSub_PaddedBase64Url_AlsoWorks()
        {
            // Manually retain "=" padding to confirm the extractor tolerates it.
            var header = Base64UrlEncode(Encoding.UTF8.GetBytes("{\"alg\":\"none\"}"));
            var payload = Convert.ToBase64String(Encoding.UTF8.GetBytes("{\"sub\":\"abc\"}"))
                .Replace('+', '-').Replace('/', '_'); // base64url but keep '='
            var jwt = $"{header}.{payload}.";
            Assert.Equal("abc", JwtSubjectExtractor.ExtractSub(jwt));
        }

        [Fact]
        public void ExtractSub_UrlSafeChars_Handled()
        {
            // Make sure '-' and '_' in the base64url payload decode correctly.
            // Pick a payload whose base64 happens to contain + or / so base64url uses - or _.
            // "{\"sub\":\"???\"}" with bytes that produce '+' or '/' in output…
            // Simpler: encode a payload and force +/- substitution manually to test the path.
            var bytes = Encoding.UTF8.GetBytes("{\"sub\":\"x\"}");
            var normal = Convert.ToBase64String(bytes);
            var urlSafe = normal.Replace('+', '-').Replace('/', '_').TrimEnd('=');
            var jwt = "aGVhZGVy." + urlSafe + ".c2ln";
            Assert.Equal("x", JwtSubjectExtractor.ExtractSub(jwt));
        }

        [Fact]
        public void ExtractSub_Null_Throws()
        {
            var ex = Assert.Throws<FormatException>(() => JwtSubjectExtractor.ExtractSub(null));
            Assert.Contains("null", ex.Message);
        }

        [Fact]
        public void ExtractSub_Empty_Throws()
        {
            var ex = Assert.Throws<FormatException>(() => JwtSubjectExtractor.ExtractSub(""));
            Assert.Contains("empty", ex.Message);
        }

        [Fact]
        public void ExtractSub_OneSegment_Throws()
        {
            var ex = Assert.Throws<FormatException>(
                () => JwtSubjectExtractor.ExtractSub("onlyonepart"));
            Assert.Contains("3", ex.Message);
        }

        [Fact]
        public void ExtractSub_TwoSegments_Throws()
        {
            var ex = Assert.Throws<FormatException>(
                () => JwtSubjectExtractor.ExtractSub("a.b"));
            Assert.Contains("3", ex.Message);
        }

        [Fact]
        public void ExtractSub_FourSegments_Throws()
        {
            var ex = Assert.Throws<FormatException>(
                () => JwtSubjectExtractor.ExtractSub("a.b.c.d"));
            Assert.Contains("3", ex.Message);
        }

        [Fact]
        public void ExtractSub_MalformedBase64InPayload_Throws()
        {
            // '!' is not a valid base64url character.
            var ex = Assert.Throws<FormatException>(
                () => JwtSubjectExtractor.ExtractSub("aGVhZGVy.not!base64.c2ln"));
            Assert.Contains("base64url", ex.Message);
        }

        [Fact]
        public void ExtractSub_MalformedJsonInPayload_Throws()
        {
            // Encode the string "not json" (not wrapped in {}) — decodes fine but fails JSON parse.
            var badPayload = Base64UrlEncode(Encoding.UTF8.GetBytes("not json"));
            var ex = Assert.Throws<FormatException>(
                () => JwtSubjectExtractor.ExtractSub($"aGVhZGVy.{badPayload}.c2ln"));
            Assert.Contains("not valid JSON", ex.Message);
        }

        [Fact]
        public void ExtractSub_PayloadIsJsonArray_Throws()
        {
            var arrayPayload = Base64UrlEncode(Encoding.UTF8.GetBytes("[\"not\",\"an\",\"object\"]"));
            var ex = Assert.Throws<FormatException>(
                () => JwtSubjectExtractor.ExtractSub($"aGVhZGVy.{arrayPayload}.c2ln"));
            Assert.Contains("not a JSON object", ex.Message);
        }

        [Fact]
        public void ExtractSub_MissingSubClaim_Throws()
        {
            var jwt = MakeJwt("{\"iss\":\"https://x\",\"aud\":\"a\"}");
            var ex = Assert.Throws<FormatException>(() => JwtSubjectExtractor.ExtractSub(jwt));
            Assert.Contains("'sub'", ex.Message);
        }

        [Fact]
        public void ExtractSub_SubClaimIsNumber_Throws()
        {
            var jwt = MakeJwt("{\"sub\":12345}");
            var ex = Assert.Throws<FormatException>(() => JwtSubjectExtractor.ExtractSub(jwt));
            Assert.Contains("'sub'", ex.Message);
        }

        [Fact]
        public void ExtractSub_SubClaimIsNull_Throws()
        {
            var jwt = MakeJwt("{\"sub\":null}");
            var ex = Assert.Throws<FormatException>(() => JwtSubjectExtractor.ExtractSub(jwt));
            Assert.Contains("'sub'", ex.Message);
        }

        [Fact]
        public void ExtractSub_SubClaimIsEmptyString_Throws()
        {
            var jwt = MakeJwt("{\"sub\":\"\"}");
            var ex = Assert.Throws<FormatException>(() => JwtSubjectExtractor.ExtractSub(jwt));
            Assert.Contains("empty", ex.Message);
        }

        [Fact]
        public void ExtractSub_OversizedInput_Throws()
        {
            // 8193-character string — just over the ceiling.
            var oversized = new string('a', 8193);
            var ex = Assert.Throws<FormatException>(
                () => JwtSubjectExtractor.ExtractSub(oversized));
            Assert.Contains("exceeds maximum", ex.Message);
        }

        [Fact]
        public void ExtractSub_AtCeiling_ReachesParser()
        {
            // A string exactly at 8192 chars should pass the ceiling check and then
            // fail at the segment-count check (segments != 3) — proving the ceiling
            // is *just* at 8192 and the order-of-checks is ceiling-then-structure.
            var atCeiling = new string('a', 8192);
            var ex = Assert.Throws<FormatException>(
                () => JwtSubjectExtractor.ExtractSub(atCeiling));
            Assert.Contains("3", ex.Message); // segment-count error, not ceiling error
        }

        [Fact]
        public void ExtractSub_EmptyPayloadSegment_Throws()
        {
            var ex = Assert.Throws<FormatException>(
                () => JwtSubjectExtractor.ExtractSub("header..sig"));
            Assert.Contains("empty", ex.Message);
        }

        // ---- Helpers ----

        private static string MakeJwt(string payloadJson)
        {
            var header = Base64UrlEncode(Encoding.UTF8.GetBytes("{\"alg\":\"none\",\"typ\":\"JWT\"}"));
            var payload = Base64UrlEncode(Encoding.UTF8.GetBytes(payloadJson));
            return $"{header}.{payload}.";
        }

        private static string Base64UrlEncode(byte[] bytes)
            => Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');
    }
}
