using System;
using System.Threading.Tasks;
using Azure.Core;
using Azure.Security.KeyVault.Keys.Cryptography;

namespace Confluent.SchemaRegistry.Encryption.Azure
{
    public class AzureKmsClient : IKmsClient
    {
        private CryptographyClient kmsClient;
        private TokenCredential credentials;
        private string keyId;
        
        public string KekId { get; }
        
        public AzureKmsClient(string kekId, TokenCredential tokenCredential)
        {
            KekId = kekId;
            if (!kekId.StartsWith(AzureKmsDriver.Prefix)) {
              throw new ArgumentException(string.Format($"key URI must start with {AzureKmsDriver.Prefix}"));
            }
            keyId = KekId.Substring(AzureKmsDriver.Prefix.Length);
            credentials = tokenCredential;
        }
        
        public bool DoesSupport(string uri)
        {
            return uri.StartsWith(AzureKmsDriver.Prefix); 
        }
        
        public async Task<byte[]> Encrypt(byte[] plaintext)
        {
            var client = GetCryptographyClient();
            var result = await client.EncryptAsync(EncryptionAlgorithm.RsaOaep, plaintext).ConfigureAwait(false);
            return result.Ciphertext;
        }

        public async Task<byte[]> Decrypt(byte[] ciphertext)
        {
            var client = GetCryptographyClient();
            var result = await client.DecryptAsync(EncryptionAlgorithm.RsaOaep, ciphertext).ConfigureAwait(false);
            return result.Plaintext;
        }
        
        private CryptographyClient GetCryptographyClient()
        {
            if (kmsClient == null)
            {
                kmsClient = new CryptographyClient(new Uri(keyId), credentials);
            }
            return kmsClient;
        }
    }
}