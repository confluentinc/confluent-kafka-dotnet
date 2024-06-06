using System;
using System.IO;
using System.Security.Cryptography;
using Miscreant;

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
            return Aead.GenerateNonce(KeySize());
        }

        public byte[] Encrypt(byte[] key, byte[] plaintext)
        {
            switch (DekFormat)
            {
                case DekFormat.AES256_SIV:
                    return EncryptWithAesSiv(key, plaintext);
                case DekFormat.AES128_GCM:
                case DekFormat.AES256_GCM:
                    return EncryptWithAesGcm(key, plaintext);
                default:
                    throw new ArgumentException();
            }
        }

        public byte[] Decrypt(byte[] key, byte[] ciphertext)
        {
            switch (DekFormat)
            {
                case DekFormat.AES256_SIV:
                    return DecryptWithAesSiv(key, ciphertext);
                case DekFormat.AES128_GCM:
                case DekFormat.AES256_GCM:
                    return DecryptWithAesGcm(key, ciphertext);
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
            using (var aes = new AesGcm(key))
            {
                var nonce = new byte[AesGcm.NonceByteSizes.MaxSize];
                RandomNumberGenerator.Fill(nonce);

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

            using (var aes = new AesGcm(key))
            {
                var plaintextBytes = new byte[ciphertext.Length];

                aes.Decrypt(nonce, ciphertext, tag, plaintextBytes);

                return plaintextBytes;
            }
        }
    }
}