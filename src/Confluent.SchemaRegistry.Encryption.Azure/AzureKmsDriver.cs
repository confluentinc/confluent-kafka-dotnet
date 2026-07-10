using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Azure.Core;
using Azure.Identity;
using Azure.Security.KeyVault.Keys;

namespace Confluent.SchemaRegistry.Encryption.Azure
{
    public class AzureKmsDriver : IKmsDriver
    {
        public static void Register()
        {
            KmsRegistry.RegisterKmsDriver(new AzureKmsDriver());
        }

        public static readonly string Prefix = "azure-kms://";
        public static readonly string TenantId = "tenant.id";
        public static readonly string ClientId = "client.id";
        public static readonly string ClientSecret = "client.secret";

        /// <summary>
        ///     Enables making a DEK's encrypted key material self-describing with respect to which
        ///     exact Azure Key Vault key version wrapped it (see <see cref="AzureKmsClient" />),
        ///     matching the same self-description property AWS KMS and GCP KMS ciphertext already
        ///     provide natively. Set as a kek KmsProps entry.
        /// </summary>
        public static readonly string EncryptAzureKeyVersionSave = "encrypt.azure.key.version.save";

        public string GetKeyUrlPrefix()
        {
            return Prefix;
        }

        public IKmsClient NewKmsClient(IDictionary<string, string> config, string keyUrl)
        {
            TokenCredential credential = GetCredentials(config);
            return new AzureKmsClient(keyUrl, credential, config);
        }

        internal static TokenCredential GetCredentials(IDictionary<string, string> config)
        {
            if (config.TryGetValue(TenantId, out string tenantId)
                && config.TryGetValue(ClientId, out string clientId)
                && config.TryGetValue(ClientSecret, out string clientSecret))
            {
                return new ClientSecretCredential(tenantId, clientId, clientSecret);
            }
            return new DefaultAzureCredential();
        }

        internal struct KeyVaultId
        {
            public string VaultUrl;
            public string Name;
            public string Version;
        }

        /// <summary>
        ///     Parses a possibly-versionless Azure Key Vault key identifier (e.g.
        ///     "https://vault.vault.azure.net/keys/name" or ".../name/&lt;version&gt;") into its
        ///     vault URL, key name and (possibly null) version.
        /// </summary>
        internal static KeyVaultId Parse(string kmsKeyId)
        {
            Uri uri;
            try
            {
                uri = new Uri(kmsKeyId);
            }
            catch (UriFormatException e)
            {
                throw new ArgumentException($"Invalid Azure Key Vault key id: {kmsKeyId}", e);
            }
            var segments = uri.AbsolutePath.Split(new[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
            if (segments.Length < 2 || segments.Length > 3 || segments[0] != "keys")
            {
                throw new ArgumentException($"Invalid Azure Key Vault key id: {kmsKeyId}");
            }
            var vaultUrl = $"{uri.Scheme}://{uri.Authority}";
            return new KeyVaultId
            {
                VaultUrl = vaultUrl,
                Name = segments[1],
                Version = segments.Length == 3 ? segments[2] : null
            };
        }

        /// <summary>
        ///     Returns true if <paramref name="kmsKeyId" /> has no explicit version segment. Used
        ///     to warn when EncryptAzureKeyVersionSave is not enabled for a versionless key,
        ///     without performing any actual resolution (no KeyClient call).
        /// </summary>
        public static bool IsVersionless(string kmsKeyId)
        {
            return Parse(kmsKeyId).Version == null;
        }

        /// <summary>
        ///     Combines <paramref name="kmsKeyId" /> (versionless or versioned; only the vault and
        ///     key name are used) with an explicit <paramref name="version" />, returning the full
        ///     versioned key identifier. Used to reconstruct a target for a version extracted from
        ///     an already-wrapped DEK, which may differ from whatever GetVersionedKeyId currently
        ///     resolves to (e.g. after a rotation).
        /// </summary>
        public static string WithVersion(string kmsKeyId, string version)
        {
            var parsed = Parse(kmsKeyId);
            return $"{parsed.VaultUrl}/keys/{parsed.Name}/{version}";
        }

        /// <summary>
        ///     Resolves a possibly-versionless Azure Key Vault key identifier into the concrete,
        ///     currently-enabled version. If <paramref name="kmsKeyId" /> already includes a
        ///     version segment, it is returned unchanged and no call is made.
        ///
        ///     This exists because, unlike AWS KMS and GCP KMS, Azure Key Vault's wrap/unwrap
        ///     operations address an explicit key version and do not embed that version in the
        ///     returned ciphertext, so a caller that only ever uses a versionless reference has no
        ///     way to know which version encrypted a given DEK once the key has been rotated.
        /// </summary>
        public static async Task<string> GetVersionedKeyId(IDictionary<string, string> config, string kmsKeyId)
        {
            var parsed = Parse(kmsKeyId);
            if (parsed.Version != null)
            {
                // Already versioned; respect the explicitly pinned config as-is.
                return kmsKeyId;
            }
            var client = new KeyClient(new Uri(parsed.VaultUrl), GetCredentials(config));
            KeyVaultKey key;
            try
            {
                var response = await client.GetKeyAsync(parsed.Name).ConfigureAwait(false);
                key = response.Value;
            }
            catch (Exception e)
            {
                throw new ArgumentException(
                    $"Failed to resolve Azure Key Vault key id for key name '{parsed.Name}' in vault {parsed.VaultUrl}",
                    e);
            }
            if (key?.Id == null)
            {
                throw new ArgumentException(
                    $"Failed to resolve Azure Key Vault key id for key name '{parsed.Name}' in vault {parsed.VaultUrl}");
            }
            var resolvedId = key.Id.ToString();
            if (Parse(resolvedId).Version == null)
            {
                throw new ArgumentException($"Resolved Azure Key Vault key id is missing a version segment: {resolvedId}");
            }
            return resolvedId;
        }
    }
}
