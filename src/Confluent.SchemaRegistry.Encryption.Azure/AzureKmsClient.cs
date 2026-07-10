using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Azure.Core;
using Azure.Security.KeyVault.Keys.Cryptography;

namespace Confluent.SchemaRegistry.Encryption.Azure
{
    /// <summary>
    ///     Basic Azure client for encryption/decryption.
    ///
    ///     Unlike AWS KMS and GCP KMS, Azure Key Vault addresses wrap/unwrap by an explicit key
    ///     version and does not embed that version in the ciphertext it returns. When
    ///     <see cref="AzureKmsDriver.EncryptAzureKeyVersionSave" /> is enabled, <see cref="Encrypt" />
    ///     makes its output self-describing by prepending the exact version that produced it:
    ///     "azure:v1:" + 32-character key version + ":" + raw ciphertext bytes.
    ///
    ///     <see cref="Decrypt" /> always checks for this prefix regardless of the current toggle
    ///     value, since a DEK wrapped while the toggle was on must remain decryptable even after it
    ///     is turned back off.
    /// </summary>
    public class AzureKmsClient : IKmsClient
    {
        private static readonly byte[] Prefix = Encoding.ASCII.GetBytes("azure:v1:");
        private const int VersionLength = 32;
        private static readonly int HeaderLength = Prefix.Length + VersionLength + 1; // +1 for ':'

        private readonly TokenCredential credentials;
        private readonly string keyId;
        private readonly IDictionary<string, string> config;

        // Built from the raw (possibly versionless) keyId, exactly as before this feature existed:
        // used directly whenever SaveVersion is off, and as Decrypt's fallback for legacy
        // ciphertext with no embedded version. Cheap to build eagerly: the constructor does not
        // itself make a network call (the Azure SDK resolves lazily on the first actual
        // encrypt/decrypt call).
        private readonly CryptographyClient defaultClient;

        public string KekId { get; }

        public AzureKmsClient(string kekId, TokenCredential tokenCredential, IDictionary<string, string> config = null)
        {
            KekId = kekId;
            if (!kekId.StartsWith(AzureKmsDriver.Prefix))
            {
                throw new ArgumentException(string.Format($"key URI must start with {AzureKmsDriver.Prefix}"));
            }
            keyId = KekId.Substring(AzureKmsDriver.Prefix.Length);
            credentials = tokenCredential;
            this.config = config ?? new Dictionary<string, string>();
            defaultClient = new CryptographyClient(new Uri(keyId), credentials);
        }

        public bool DoesSupport(string uri)
        {
            return KekId == uri;
        }

        private bool SaveVersion =>
            config.TryGetValue(AzureKmsDriver.EncryptAzureKeyVersionSave, out var value)
            && bool.TryParse(value, out var parsed) && parsed;

        // Builds a CryptographyClient for an explicit version. Used both by Encrypt (once it has
        // resolved the current version) and Decrypt (to target whichever version is embedded in
        // already-wrapped ciphertext) -- there is only one place that knows how to turn a version
        // into a client.
        private CryptographyClient ClientForVersion(string version)
        {
            var versionedKeyUri = AzureKmsDriver.WithVersion(keyId, version);
            return new CryptographyClient(new Uri(versionedKeyUri), credentials);
        }

        public async Task<byte[]> Encrypt(byte[] plaintext)
        {
            if (!SaveVersion)
            {
                var result = await defaultClient.EncryptAsync(EncryptionAlgorithm.RsaOaep256, plaintext)
                    .ConfigureAwait(false);
                return result.Ciphertext;
            }
            var resolvedKeyUri = await AzureKmsDriver.GetVersionedKeyId(config, keyId).ConfigureAwait(false);
            var version = resolvedKeyUri.Substring(resolvedKeyUri.LastIndexOf('/') + 1);
            if (!IsValidVersion(version))
            {
                // Mirrors Decrypt's own validation: a DEK this method wraps must always be one this
                // same class can later unwrap.
                throw new ArgumentException(
                    $"kms key version '{version}' must be a {VersionLength}-character hex string; " +
                    "cannot be embedded in a fixed-width azure:v1: prefix");
            }
            var client = ClientForVersion(version);
            var encryptResult = await client.EncryptAsync(EncryptionAlgorithm.RsaOaep256, plaintext)
                .ConfigureAwait(false);
            var ciphertext = encryptResult.Ciphertext;
            var versionBytes = Encoding.ASCII.GetBytes(version);
            var output = new byte[Prefix.Length + versionBytes.Length + 1 + ciphertext.Length];
            Buffer.BlockCopy(Prefix, 0, output, 0, Prefix.Length);
            Buffer.BlockCopy(versionBytes, 0, output, Prefix.Length, versionBytes.Length);
            output[Prefix.Length + versionBytes.Length] = (byte) ':';
            Buffer.BlockCopy(ciphertext, 0, output, Prefix.Length + versionBytes.Length + 1, ciphertext.Length);
            return output;
        }

        public async Task<byte[]> Decrypt(byte[] ciphertext)
        {
            var client = defaultClient;
            var wrapped = ciphertext;
            var version = ExtractVersion(ciphertext);
            if (version != null)
            {
                if (!IsValidVersion(version))
                {
                    // Encrypted key material is unauthenticated at this layer, so a corrupted or
                    // tampered value could otherwise smuggle arbitrary characters (e.g. '/') into
                    // the key identifier URL built from it below.
                    throw new ArgumentException($"ciphertext carries an invalid azure:v1: key version: '{version}'");
                }
                client = ClientForVersion(version);
                wrapped = new byte[ciphertext.Length - HeaderLength];
                Buffer.BlockCopy(ciphertext, HeaderLength, wrapped, 0, wrapped.Length);
            }
            var result = await client.DecryptAsync(EncryptionAlgorithm.RsaOaep256, wrapped).ConfigureAwait(false);
            return result.Plaintext;
        }

        internal static bool IsValidVersion(string value)
        {
            if (value == null || value.Length != VersionLength)
            {
                return false;
            }
            foreach (var c in value)
            {
                bool isHex = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
                if (!isHex)
                {
                    return false;
                }
            }
            return true;
        }

        // Returns the embedded version if ciphertext carries the "azure:v1:" prefix (see class
        // doc), or null if it does not (e.g. a legacy DEK wrapped before
        // EncryptAzureKeyVersionSave was enabled on its KEK, or the toggle is not set). Returning
        // null rather than throwing is deliberate: the toggle can be flipped on/off over a KEK's
        // lifetime, and old, un-prefixed ciphertext must remain decryptable.
        internal static string ExtractVersion(byte[] ciphertext)
        {
            if (ciphertext.Length < HeaderLength || ciphertext[HeaderLength - 1] != (byte) ':')
            {
                return null;
            }
            for (int i = 0; i < Prefix.Length; i++)
            {
                if (ciphertext[i] != Prefix[i])
                {
                    return null;
                }
            }
            return Encoding.ASCII.GetString(ciphertext, Prefix.Length, VersionLength);
        }
    }
}
