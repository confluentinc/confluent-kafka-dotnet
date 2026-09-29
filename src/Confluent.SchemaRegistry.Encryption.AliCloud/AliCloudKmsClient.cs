using System;
using System.Text;
using System.Threading.Tasks;
using AlibabaCloud.SDK.Kms20160120;
using AlibabaCloud.SDK.Kms20160120.Models;
using AlibabaCloud.OpenApiClient.Models;
using AliCloudCredential = Aliyun.Credentials.Client;

namespace Confluent.SchemaRegistry.Encryption.AliCloud
{
    public class AliCloudKmsClient : IKmsClient
    {
        private readonly Client kmsClient;
        private readonly string keyId;
        public string KekId { get; }

        public AliCloudKmsClient(string kekId, AliCloudCredential credential, string endpoint = null, string caCert = null)
        {
            KekId = kekId;

            if (!kekId.StartsWith(AliCloudKmsDriver.Prefix))
            {
              throw new ArgumentException(string.Format($"key URI must start with {AliCloudKmsDriver.Prefix}"));
            }

            string keyUri = KekId.Substring(AliCloudKmsDriver.Prefix.Length);
            int slashIndex = keyUri.IndexOf('/');
            if (slashIndex <= 0 || slashIndex == keyUri.Length - 1)
            {
                throw new ArgumentException("key URI must be of the form alicloud-kms://<region>/<key>");
            }

            string regionId = keyUri.Substring(0, slashIndex);
            keyId = keyUri.Substring(slashIndex + 1);

            var config = new Config
            {
                Credential = credential,
                RegionId = regionId,
                Protocol = "https",
            };
            if (endpoint != null)
            {
                config.Endpoint = endpoint;
            }
            if (caCert != null)
            {
                config.Ca = caCert;
            }
            kmsClient = new Client(config);
        }

        public bool DoesSupport(string uri) => KekId.Equals(uri);

        public async Task<byte[]> Encrypt(byte[] plaintext)
        {
            var request = new EncryptRequest
            {
                KeyId = keyId,
                Plaintext = Convert.ToBase64String(plaintext)
            };
            var response = await kmsClient.EncryptAsync(request).ConfigureAwait(false);
            return Encoding.UTF8.GetBytes(response.Body.CiphertextBlob);
        }

        public async Task<byte[]> Decrypt(byte[] ciphertext)
        {
            var request = new DecryptRequest
            {
                CiphertextBlob = Encoding.UTF8.GetString(ciphertext)
            };
            var response = await kmsClient.DecryptAsync(request).ConfigureAwait(false);
            return Convert.FromBase64String(response.Body.Plaintext);
        }
    }
}
