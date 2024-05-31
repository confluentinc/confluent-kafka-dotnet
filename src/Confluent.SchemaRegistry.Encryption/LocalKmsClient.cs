using System;
using HkdfStandard;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace Confluent.SchemaRegistry.Encryption
{
    public class LocalKmsClient : IKmsClient
    {
        public string Secret { get; }
        private Cryptor cryptor;
        private byte[] key;

        public LocalKmsClient(string secret)
        {
            if (secret == null)
            {
                secret = Environment.GetEnvironmentVariable("LOCAL_SECRET");
            }
            Secret = secret;
            cryptor = new Cryptor(DekFormat.AES256_GCM);
            key = Hkdf.DeriveKey(HashAlgorithmName.SHA256, Encoding.UTF8.GetBytes(secret), cryptor.KeySize());
        }

        public bool DoesSupport(string uri)
        {
            return uri.StartsWith(LocalKmsDriver.Prefix);
        }
        
        public Task<byte[]> Encrypt(byte[] plaintext)
        {
            return Task.FromResult(cryptor.Encrypt(key, plaintext));
        }

        public Task<byte[]> Decrypt(byte[] ciphertext)
        {
            return Task.FromResult(cryptor.Decrypt(key, ciphertext));
        }
    }
}