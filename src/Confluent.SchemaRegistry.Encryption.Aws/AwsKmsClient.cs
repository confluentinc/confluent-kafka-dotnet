using System;
using System.IO;
using System.Threading.Tasks;
using Amazon;
using Amazon.KeyManagementService;
using Amazon.KeyManagementService.Model;
using Amazon.Runtime;

namespace Confluent.SchemaRegistry.Encryption.Aws
{
    public class AwsKmsClient : IKmsClient
    {
        private AmazonKeyManagementServiceClient kmsClient;
        private string keyId;
        
        public string KekId { get; }
        
        public AwsKmsClient(string kekId, AWSCredentials credentials)
        {
            KekId = kekId;
            
            if (!kekId.StartsWith(AwsKmsDriver.Prefix)) {
              throw new ArgumentException(string.Format($"key URI must start with {AwsKmsDriver.Prefix}"));
            }
            keyId = KekId.Substring(AwsKmsDriver.Prefix.Length);
            string[] tokens = keyId.Split(':');
            if (tokens.Length < 4) {
                throw new ArgumentException("invalid key URI");
            }
            string regionName = tokens[3];
            RegionEndpoint regionEndpoint = RegionEndpoint.GetBySystemName(regionName);
            kmsClient = credentials != null
                ? new AmazonKeyManagementServiceClient(credentials, regionEndpoint)
                : new AmazonKeyManagementServiceClient(regionEndpoint);
        }

        public bool DoesSupport(string uri)
        {
            return uri.StartsWith(AwsKmsDriver.Prefix); 
        }
        
        public async Task<byte[]> Encrypt(byte[] plaintext)
        {
            using var dataStream = new MemoryStream(plaintext);
            var request = new EncryptRequest
            {
                KeyId = keyId,
                Plaintext = dataStream
            };
            var response = await kmsClient.EncryptAsync(request).ConfigureAwait(false);
            return response.CiphertextBlob.ToArray();
        }

        public async Task<byte[]> Decrypt(byte[] ciphertext)
        {
            using var dataStream = new MemoryStream(ciphertext);
            var request = new DecryptRequest
            {
                KeyId = keyId,
                CiphertextBlob = dataStream
            };
            var response = await kmsClient.DecryptAsync(request).ConfigureAwait(false);
            return response.Plaintext.ToArray();
        }
    }
}