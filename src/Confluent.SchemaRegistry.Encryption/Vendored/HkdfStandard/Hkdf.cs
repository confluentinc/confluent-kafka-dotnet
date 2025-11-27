using System;
using System.Security.Cryptography;

namespace Confluent.SchemaRegistry.Encryption.Vendored.HkdfStandard
{
    /// <summary>
    /// Simplified HKDF implementation for use in the Confluent Schema Registry Encryption library.
    /// This is a subset of the original HKDF.Standard library, containing only the methods needed.
    /// Original source: https://github.com/andreimilto/HKDF.Standard
    /// </summary>
    public static class Hkdf
    {
        /// <summary>
        /// Derives a key from the provided input key material using HKDF.
        /// </summary>
        /// <param name="hashAlgorithmName">The hash algorithm to use.</param>
        /// <param name="ikm">The input key material.</param>
        /// <param name="outputLength">The desired length of the output key in bytes.</param>
        /// <param name="salt">Optional salt value (defaults to hash output length of zeros if null).</param>
        /// <param name="info">Optional context information (defaults to empty if null).</param>
        /// <returns>The derived key.</returns>
        public static byte[] DeriveKey(HashAlgorithmName hashAlgorithmName, byte[] ikm, int outputLength, byte[] salt = null, byte[] info = null)
        {
            if (ikm == null)
                throw new ArgumentNullException(nameof(ikm));
            if (outputLength <= 0)
                throw new ArgumentOutOfRangeException(nameof(outputLength));

            // Extract
            byte[] prk = Extract(hashAlgorithmName, ikm, salt);
            
            // Expand
            return Expand(hashAlgorithmName, prk, outputLength, info);
        }

        /// <summary>
        /// HKDF Extract step - extracts a pseudorandom key from input key material.
        /// </summary>
        private static byte[] Extract(HashAlgorithmName hashAlgorithmName, byte[] ikm, byte[] salt)
        {
            int hashLength = GetHashLength(hashAlgorithmName);
            
            // Use a salt of HashLen zeros if not provided
            if (salt == null || salt.Length == 0)
            {
                salt = new byte[hashLength];
            }

            using (var hmac = CreateHMAC(hashAlgorithmName, salt))
            {
                return hmac.ComputeHash(ikm);
            }
        }

        /// <summary>
        /// HKDF Expand step - expands the pseudorandom key to desired length.
        /// </summary>
        private static byte[] Expand(HashAlgorithmName hashAlgorithmName, byte[] prk, int outputLength, byte[] info)
        {
            int hashLength = GetHashLength(hashAlgorithmName);
            
            if (prk.Length < hashLength)
                throw new ArgumentException("PRK must be at least HashLen bytes.", nameof(prk));
            
            int n = (outputLength + hashLength - 1) / hashLength; // Ceiling division
            if (n > 255)
                throw new ArgumentOutOfRangeException(nameof(outputLength), "Output length too large.");

            if (info == null)
                info = new byte[0];

            byte[] okm = new byte[outputLength];
            byte[] t = new byte[0];
            int offset = 0;

            using (var hmac = CreateHMAC(hashAlgorithmName, prk))
            {
                for (byte i = 1; i <= n; i++)
                {
                    byte[] input = new byte[t.Length + info.Length + 1];
                    Buffer.BlockCopy(t, 0, input, 0, t.Length);
                    Buffer.BlockCopy(info, 0, input, t.Length, info.Length);
                    input[input.Length - 1] = i;

                    t = hmac.ComputeHash(input);
                    
                    int bytesToCopy = Math.Min(t.Length, outputLength - offset);
                    Buffer.BlockCopy(t, 0, okm, offset, bytesToCopy);
                    offset += bytesToCopy;
                }
            }

            return okm;
        }

        /// <summary>
        /// Gets the hash length in bytes for the specified algorithm.
        /// </summary>
        private static int GetHashLength(HashAlgorithmName hashAlgorithmName)
        {
            if (hashAlgorithmName == HashAlgorithmName.SHA1)
                return 20;
            if (hashAlgorithmName == HashAlgorithmName.SHA256)
                return 32;
            if (hashAlgorithmName == HashAlgorithmName.SHA384)
                return 48;
            if (hashAlgorithmName == HashAlgorithmName.SHA512)
                return 64;
            
            throw new ArgumentOutOfRangeException(nameof(hashAlgorithmName), "Unsupported hash algorithm.");
        }

        /// <summary>
        /// Creates an HMAC instance for the specified algorithm with the given key.
        /// </summary>
        private static HMAC CreateHMAC(HashAlgorithmName hashAlgorithmName, byte[] key)
        {
            if (hashAlgorithmName == HashAlgorithmName.SHA1)
                return new HMACSHA1(key);
            if (hashAlgorithmName == HashAlgorithmName.SHA256)
                return new HMACSHA256(key);
            if (hashAlgorithmName == HashAlgorithmName.SHA384)
                return new HMACSHA384(key);
            if (hashAlgorithmName == HashAlgorithmName.SHA512)
                return new HMACSHA512(key);
            
            throw new ArgumentOutOfRangeException(nameof(hashAlgorithmName), "Unsupported hash algorithm.");
        }
    }
}
