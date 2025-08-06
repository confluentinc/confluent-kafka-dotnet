using System;
using System.Text;
using System.Threading.Tasks;
using VaultSharp;
using VaultSharp.V1.AuthMethods;
using VaultSharp.V1.AuthMethods.Token;
using VaultSharp.V1.Commons;
using VaultSharp.V1.SecretsEngines.Transit;

namespace Confluent.SchemaRegistry.Encryption.HcVault
{
    public class HcVaultKmsClient : IKmsClient
    {
        private IVaultClient kmsClient;
        private string keyId;
        private string keyName;
        
        public string KekId { get; }
        public string Namespace { get; }
        public string TokenId { get; }
        
        public HcVaultKmsClient(string kekId, string ns, string tokenId)
        {
            KekId = kekId;
            Namespace = ns;
            TokenId = tokenId;
            
            if (!kekId.StartsWith(HcVaultKmsDriver.Prefix))
            {
              throw new ArgumentException(string.Format($"key URI must start with {HcVaultKmsDriver.Prefix}"));
            }
            keyId = KekId.Substring(HcVaultKmsDriver.Prefix.Length);
            IAuthMethodInfo authMethod = new TokenAuthMethodInfo(tokenId);
            Uri uri = new Uri(keyId);
            if (uri.Segments.Length == 0)
            {
              throw new ArgumentException(string.Format($"key URI must contain a key name"));
            }
            keyName = uri.Segments[uri.Segments.Length - 1];

            var vaultClientSettings = new VaultClientSettings(uri.Scheme + "://" + uri.Authority, authMethod);
            if (ns != null)
            {
                vaultClientSettings.Namespace = ns;
            }
            kmsClient = new VaultClient(vaultClientSettings);
        }
        
        public bool DoesSupport(string uri)
        {
            return KekId == uri;
        }
        
        public async Task<byte[]> Encrypt(byte[] plaintext)
        {
            var encodedPlaintext = Convert.ToBase64String(plaintext);
            var encryptOptions = new EncryptRequestOptions
            {
                Base64EncodedPlainText = encodedPlaintext
            };

            Secret<EncryptionResponse> encryptionResponse = await kmsClient.V1.Secrets.Transit.EncryptAsync(keyName, encryptOptions)
                .ConfigureAwait(false);
            return Encoding.UTF8.GetBytes(encryptionResponse.Data.CipherText);
        }

        public async Task<byte[]> Decrypt(byte[] ciphertext)
        {
            var encodedCiphertext = Encoding.UTF8.GetString(ciphertext);
            var decryptOptions = new DecryptRequestOptions
            {
                CipherText = encodedCiphertext
            };

            Secret<DecryptionResponse> decryptionResponse = await kmsClient.V1.Secrets.Transit.DecryptAsync(keyName, decryptOptions)
                .ConfigureAwait(false);
            return Convert.FromBase64String(decryptionResponse.Data.Base64EncodedPlainText);
        }
    }
}