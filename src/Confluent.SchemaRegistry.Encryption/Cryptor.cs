using System;
using System.IO;
using System.Security.Cryptography;
using Google.Crypto.Tink;
using Google.Protobuf;
using Confluent.SchemaRegistry.Encryption.Vendored.Miscreant;

namespace Confluent.SchemaRegistry.Encryption
{
    public class Cryptor
    {
        private static byte[] EmptyAAD = new byte[] { };
        
        public Cryptor(DekFormat dekFormat)
        {
            DekFormat = dekFormat;
            IsDeterministic = dekFormat == DekFormat.AES256_SIV;
        }

        public DekFormat DekFormat { get; private set; }

        public bool IsDeterministic { get; private set; }

        public int KeySize()
        {
            switch (DekFormat)
            {
                case DekFormat.AES256_SIV:
                    // Generate 2 256-bit keys
                    return 64;
                case DekFormat.AES128_GCM:
                    // Generate 128-bit key
                    return 16;
                case DekFormat.AES256_GCM:
                    // Generate 256-bit key
                    return 32;
                default:
                    throw new ArgumentException();
            }
            
        }

        public byte[] GenerateKey()
        {
            byte[] rawKey = Aead.GenerateNonce(KeySize());
            switch (DekFormat)
            {
                case DekFormat.AES256_SIV:
                    AesSivKey aesSiv = new AesSivKey();
                    aesSiv.Version = 0;
                    aesSiv.KeyValue = ByteString.CopyFrom(rawKey);
                    return aesSiv.ToByteArray();
                case DekFormat.AES128_GCM:
                case DekFormat.AES256_GCM:
                    AesGcmKey aesGcm = new AesGcmKey();
                    aesGcm.Version = 0;
                    aesGcm.KeyValue = ByteString.CopyFrom(rawKey);
                    return aesGcm.ToByteArray();
                default:
                    throw new ArgumentException();
            }
        }

        public byte[] Encrypt(byte[] key, byte[] plaintext)
        {
            byte[] rawKey;
            switch (DekFormat)
            {
                case DekFormat.AES256_SIV:
                    AesSivKey aesSiv = AesSivKey.Parser.ParseFrom(key);
                    rawKey = aesSiv.KeyValue.ToByteArray();
                    return EncryptWithAesSiv(rawKey, plaintext);
                case DekFormat.AES128_GCM:
                case DekFormat.AES256_GCM:
                    AesGcmKey aesGcm = AesGcmKey.Parser.ParseFrom(key);
                    rawKey = aesGcm.KeyValue.ToByteArray();
                    return EncryptWithAesGcm(rawKey, plaintext);
                default:
                    throw new ArgumentException();
            }
        }

        public byte[] Decrypt(byte[] key, byte[] ciphertext)
        {
            byte[] rawKey;
            switch (DekFormat)
            {
                case DekFormat.AES256_SIV:
                    AesSivKey aesSiv = AesSivKey.Parser.ParseFrom(key);
                    rawKey = aesSiv.KeyValue.ToByteArray();
                    return DecryptWithAesSiv(rawKey, ciphertext);
                case DekFormat.AES128_GCM:
                case DekFormat.AES256_GCM:
                    AesGcmKey aesGcm = AesGcmKey.Parser.ParseFrom(key);
                    rawKey = aesGcm.KeyValue.ToByteArray();
                    return DecryptWithAesGcm(rawKey, ciphertext);
                default:
                    throw new ArgumentException();
            }
        }

        static byte[] EncryptWithAesSiv(byte[] key, byte[] plaintext)
        {
            using (var aead = Aead.CreateAesCmacSiv(key))
            {
                return aead.Seal(plaintext, null, EmptyAAD);
            }
        }

        public byte[] DecryptWithAesSiv(byte[] key, byte[] ciphertext)
        {
            using (var aead = Aead.CreateAesCmacSiv(key))
            {
                return aead.Open(ciphertext, null, EmptyAAD);
            }
        }

        static byte[] EncryptWithAesGcm(byte[] key, byte[] plaintext)
        {
#if NET462
            using (var aes = new AesGcm(key, AesGcm.TagByteSizes.MaxSize))
#else
            using (var aes = new AesGcm(key))
#endif
            {
                var nonce = new byte[AesGcm.NonceByteSizes.MaxSize];
                using (var rng = RandomNumberGenerator.Create())
                {
                    rng.GetBytes(nonce);
                }

                var tag = new byte[AesGcm.TagByteSizes.MaxSize];
                var ciphertext = new byte[plaintext.Length];

                aes.Encrypt(nonce, plaintext, ciphertext, tag);

                byte[] payload;
                using (MemoryStream stream = new MemoryStream())
                {
                    using (BinaryWriter writer = new BinaryWriter(stream))
                    {
                        writer.Write(nonce);
                        writer.Write(ciphertext);
                        writer.Write(tag);
                        payload = stream.ToArray();
                    }
                }
                return payload;
            }
        }

        static byte[] DecryptWithAesGcm(byte[] key, byte[] payload)
        {
            byte[] nonce, ciphertext, tag;
            int ciphertextLength = payload.Length - AesGcm.NonceByteSizes.MaxSize - AesGcm.TagByteSizes.MaxSize;
            using (MemoryStream stream = new MemoryStream(payload))
            {
                using (BinaryReader reader = new BinaryReader(stream))
                {
                    nonce = reader.ReadBytes(AesGcm.NonceByteSizes.MaxSize);
                    ciphertext = reader.ReadBytes(ciphertextLength);
                    tag = reader.ReadBytes(AesGcm.TagByteSizes.MaxSize);
                }
            }

#if NET462
            using (var aes = new AesGcm(key, AesGcm.TagByteSizes.MaxSize))
#else
            using (var aes = new AesGcm(key))
#endif
            {
                var plaintextBytes = new byte[ciphertext.Length];

                aes.Decrypt(nonce, ciphertext, tag, plaintextBytes);

                return plaintextBytes;
            }
        }
    }
}