// Copyright 2026 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.

using System;
using System.Security.Cryptography;
using Confluent.SchemaRegistry.Encryption.Vendored.HkdfStandard;
using Xunit;

namespace Confluent.SchemaRegistry.UnitTests;

public class HkdfTests
{
    /// <summary>
    /// Tests that HKDF Expand works correctly when n=255 (maximum block count).
    /// This test verifies the fix for the byte overflow bug where using 'byte i' 
    /// in the loop would cause an infinite loop when n=255.
    /// For SHA256 (32-byte hash), n=255 means outputLength = 255 * 32 = 8160 bytes.
    /// </summary>
    [Fact]
    public void DeriveKey_WithMaximumOutputLength_ShouldComplete()
    {
        // Arrange
        var ikm = new byte[32];
        for (int i = 0; i < ikm.Length; i++)
            ikm[i] = (byte)i;

        var salt = new byte[32];
        for (int i = 0; i < salt.Length; i++)
            salt[i] = (byte)(i + 100);

        var info = new byte[16];
        for (int i = 0; i < info.Length; i++)
            info[i] = (byte)(i + 200);

        // For SHA256, hash length is 32 bytes, so max output is 255 * 32 = 8160
        int maxOutputLength = 255 * 32;

        // Act - This would hang indefinitely with the byte overflow bug
        var result = Hkdf.DeriveKey(HashAlgorithmName.SHA256, ikm, maxOutputLength, salt, info);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(maxOutputLength, result.Length);
    }

    /// <summary>
    /// Tests that HKDF produces deterministic output.
    /// </summary>
    [Fact]
    public void DeriveKey_WithSameInputs_ShouldProduceSameOutput()
    {
        // Arrange
        var ikm = new byte[] { 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b,
                               0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b,
                               0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b };
        var salt = new byte[] { 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
                                0x08, 0x09, 0x0a, 0x0b, 0x0c };
        var info = new byte[] { 0xf0, 0xf1, 0xf2, 0xf3, 0xf4, 0xf5, 0xf6, 0xf7, 0xf8, 0xf9 };
        int outputLength = 42;

        // Act
        var result1 = Hkdf.DeriveKey(HashAlgorithmName.SHA256, ikm, outputLength, salt, info);
        var result2 = Hkdf.DeriveKey(HashAlgorithmName.SHA256, ikm, outputLength, salt, info);

        // Assert
        Assert.Equal(result1, result2);
    }

    /// <summary>
    /// Tests HKDF with output length that requires exactly 255 blocks.
    /// This is the edge case that would trigger the overflow bug.
    /// </summary>
    [Fact]
    public void DeriveKey_WithExactly255Blocks_ShouldComplete()
    {
        // Arrange
        var ikm = new byte[16];
        RandomNumberGenerator.Fill(ikm);

        // For SHA256, 255 blocks = 255 * 32 = 8160 bytes
        // For SHA384, 255 blocks = 255 * 48 = 12240 bytes  
        // For SHA512, 255 blocks = 255 * 64 = 16320 bytes

        // Test SHA256 at exactly 255 blocks
        int sha256MaxOutput = 255 * 32;
        var result256 = Hkdf.DeriveKey(HashAlgorithmName.SHA256, ikm, sha256MaxOutput);
        Assert.Equal(sha256MaxOutput, result256.Length);

        // Test SHA384 at exactly 255 blocks
        int sha384MaxOutput = 255 * 48;
        var result384 = Hkdf.DeriveKey(HashAlgorithmName.SHA384, ikm, sha384MaxOutput);
        Assert.Equal(sha384MaxOutput, result384.Length);

        // Test SHA512 at exactly 255 blocks
        int sha512MaxOutput = 255 * 64;
        var result512 = Hkdf.DeriveKey(HashAlgorithmName.SHA512, ikm, sha512MaxOutput);
        Assert.Equal(sha512MaxOutput, result512.Length);
    }

    /// <summary>
    /// Tests that requesting output larger than 255 blocks throws an exception.
    /// </summary>
    [Fact]
    public void DeriveKey_WithOutputLengthExceeding255Blocks_ShouldThrow()
    {
        // Arrange
        var ikm = new byte[16];
        RandomNumberGenerator.Fill(ikm);

        // For SHA256, max is 255 * 32 = 8160, so 8161 should fail
        int tooLargeOutput = 255 * 32 + 1;

        // Act & Assert
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            Hkdf.DeriveKey(HashAlgorithmName.SHA256, ikm, tooLargeOutput));
    }

    /// <summary>
    /// Tests HKDF with various output lengths to ensure correct behavior across block boundaries.
    /// </summary>
    [Theory]
    [InlineData(1)]      // Minimum output
    [InlineData(16)]     // Less than one block
    [InlineData(32)]     // Exactly one block (SHA256)
    [InlineData(33)]     // Just over one block
    [InlineData(64)]     // Two blocks
    [InlineData(100)]    // Multiple blocks
    [InlineData(254 * 32)] // 254 blocks - one less than max
    [InlineData(255 * 32)] // 255 blocks - maximum
    public void DeriveKey_WithVariousOutputLengths_ShouldProduceCorrectLength(int outputLength)
    {
        // Arrange
        var ikm = new byte[32];
        for (int i = 0; i < ikm.Length; i++)
            ikm[i] = (byte)i;

        // Act
        var result = Hkdf.DeriveKey(HashAlgorithmName.SHA256, ikm, outputLength);

        // Assert
        Assert.Equal(outputLength, result.Length);
    }
}

