using System;
using Confluent.SchemaRegistry.Encryption.Vendored.HkdfStandard;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Google.Crypto.Tink;
using Google.Protobuf;

namespace Confluent.SchemaRegistry.Encryption
{
    public class LocalKmsClient : IKmsClient
    {
        public string Secret { get; }
        private Cryptor cryptor;
        private byte[] key;

        public LocalKmsClient(string secret)
        {
            Secret = secret;
            cryptor = new Cryptor(DekFormat.AES128_GCM);
            byte[] rawKey = Hkdf.DeriveKey(
                HashAlgorithmName.SHA256, Encoding.UTF8.GetBytes(secret), cryptor.KeySize());
            AesGcmKey aesGcm = new AesGcmKey();
            aesGcm.Version = 0;
            aesGcm.KeyValue = ByteString.CopyFrom(rawKey);
            key = aesGcm.ToByteArray();
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