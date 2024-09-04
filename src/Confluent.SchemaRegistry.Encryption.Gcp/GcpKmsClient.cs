using System;
using System.Threading.Tasks;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.Kms.V1;
using Google.Protobuf;

namespace Confluent.SchemaRegistry.Encryption.Gcp
{
    public class GcpKmsClient : IKmsClient
    {
        private KeyManagementServiceClient kmsClient;
        private string keyId;
        private CryptoKeyName keyName;
        
        public string KekId { get; }

        public GcpKmsClient(string kekId, GoogleCredential credential)
        {
            KekId = kekId;

            if (!kekId.StartsWith(GcpKmsDriver.Prefix))
            {
                throw new ArgumentException(string.Format($"key URI must start with {GcpKmsDriver.Prefix}"));
            }

            keyId = KekId.Substring(GcpKmsDriver.Prefix.Length);
            keyName = CryptoKeyName.Parse(keyId);
            kmsClient = credential != null
                ? new KeyManagementServiceClientBuilder()
                    {
                        GoogleCredential = credential
                    }
                    .Build()
                : KeyManagementServiceClient.Create();
        }

        public bool DoesSupport(string uri)
        {
            return uri.StartsWith(GcpKmsDriver.Prefix);
        }
        
        public async Task<byte[]> Encrypt(byte[] plaintext)
        {
            var result = await kmsClient.EncryptAsync(keyName, ByteString.CopyFrom(plaintext))
                .ConfigureAwait(false);
            return result.Ciphertext.ToByteArray();
        }

        public async Task<byte[]> Decrypt(byte[] ciphertext)
        {
            var result = await kmsClient.DecryptAsync(keyId, ByteString.CopyFrom(ciphertext))
                .ConfigureAwait(false);
            return result.Plaintext.ToByteArray();
        }
    }
}