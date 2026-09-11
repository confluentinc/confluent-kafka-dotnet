using System;
using System.Text;
using Azure.Identity;
using Confluent.SchemaRegistry.Encryption.Azure;
using Xunit;

namespace Confluent.SchemaRegistry.UnitTests.Encryption.Azure
{
    public class AzureKmsClientTests
    {
        private const string VersionA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        private const string KeyUri = "azure-kms://https://yokota1.vault.azure.net/keys/key1";

        [Fact]
        public void ConstructorThrowsWhenPrefixMissing()
        {
            Assert.Throws<ArgumentException>(() =>
                new AzureKmsClient("https://yokota1.vault.azure.net/keys/key1", new DefaultAzureCredential()));
        }

        [Fact]
        public void DoesSupportMatchesExactKekId()
        {
            var client = new AzureKmsClient(KeyUri, new DefaultAzureCredential());
            Assert.True(client.DoesSupport(KeyUri));
            Assert.False(client.DoesSupport(KeyUri + "/other"));
        }

        [Theory]
        [InlineData(VersionA, true)]
        [InlineData("not-32-chars", false)]
        [InlineData(null, false)]
        [InlineData("", false)]
        public void IsValidVersion(string value, bool expected)
        {
            Assert.Equal(expected, AzureKmsClient.IsValidVersion(value));
        }

        [Fact]
        public void IsValidVersionRejectsNonHexCharacters()
        {
            var nonHex = "g" + VersionA.Substring(1);
            Assert.False(AzureKmsClient.IsValidVersion(nonHex));
        }

        [Fact]
        public void ExtractVersionReturnsEmbeddedVersionForPrefixedCiphertext()
        {
            var ciphertext = Encoding.ASCII.GetBytes("azure:v1:" + VersionA + ":wrapped-bytes");
            var version = AzureKmsClient.ExtractVersion(ciphertext);
            Assert.Equal(VersionA, version);
        }

        [Fact]
        public void ExtractVersionReturnsNullForLegacyUnprefixedCiphertext()
        {
            var ciphertext = Encoding.ASCII.GetBytes("legacy-unprefixed-ciphertext");
            Assert.Null(AzureKmsClient.ExtractVersion(ciphertext));
        }

        [Fact]
        public void ExtractVersionReturnsNullWhenTooShort()
        {
            var ciphertext = Encoding.ASCII.GetBytes("azure:v1:short");
            Assert.Null(AzureKmsClient.ExtractVersion(ciphertext));
        }
    }
}
