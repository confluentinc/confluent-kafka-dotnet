using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.SchemaRegistry.Encryption.Azure;
using Xunit;

namespace Confluent.SchemaRegistry.UnitTests.Encryption.Azure
{
    public class AzureKmsDriverTests
    {
        private const string VersionA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        private const string VersionlessKeyId = "https://yokota1.vault.azure.net/keys/key1";
        private static readonly string VersionedKeyId = VersionlessKeyId + "/" + VersionA;

        [Fact]
        public void IsVersionlessTrueForVersionlessId()
        {
            Assert.True(AzureKmsDriver.IsVersionless(VersionlessKeyId));
        }

        [Fact]
        public void IsVersionlessFalseForVersionedId()
        {
            Assert.False(AzureKmsDriver.IsVersionless(VersionedKeyId));
        }

        [Fact]
        public void IsVersionlessThrowsForMalformedId()
        {
            Assert.Throws<ArgumentException>(() =>
                AzureKmsDriver.IsVersionless("https://yokota1.vault.azure.net/notkeys/key1"));
        }

        [Fact]
        public void WithVersionCombinesVersionlessIdWithExplicitVersion()
        {
            Assert.Equal(VersionedKeyId, AzureKmsDriver.WithVersion(VersionlessKeyId, VersionA));
        }

        [Fact]
        public void WithVersionIgnoresExistingVersion()
        {
            // Only the vault and key name are used; any existing version segment is discarded in
            // favor of the explicit version argument.
            const string otherVersion = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
            var result = AzureKmsDriver.WithVersion(VersionedKeyId, otherVersion);
            Assert.Equal(VersionlessKeyId + "/" + otherVersion, result);
        }

        [Fact]
        public async Task GetVersionedKeyIdReturnsUnchangedWhenAlreadyVersioned()
        {
            var result = await AzureKmsDriver.GetVersionedKeyId(new Dictionary<string, string>(), VersionedKeyId);
            Assert.Equal(VersionedKeyId, result);
        }

        [Fact]
        public async Task GetVersionedKeyIdThrowsForMalformedKeyId()
        {
            await Assert.ThrowsAsync<ArgumentException>(() =>
                AzureKmsDriver.GetVersionedKeyId(
                    new Dictionary<string, string>(), "https://yokota1.vault.azure.net/notkeys/key1"));
        }

        [Fact]
        public async Task GetVersionedKeyIdThrowsForInvalidUri()
        {
            await Assert.ThrowsAsync<ArgumentException>(() =>
                AzureKmsDriver.GetVersionedKeyId(new Dictionary<string, string>(), "::not a uri::"));
        }
    }
}
