# Vendored Dependencies for Strong Naming

## Overview

The `Confluent.SchemaRegistry.Encryption` project requires all dependencies to be strongly named to maintain assembly signing compatibility. Two NuGet dependencies (`Miscreant` and `HKDF.Standard`) were not strongly named, so their source code has been vendored (copied) into this project.

## Vendored Libraries

### 1. Miscreant (v0.3.3)
- **Original Repository**: https://github.com/miscreant/miscreant.net
- **License**: MIT (see LICENSE.txt)
- **Purpose**: Provides AES-SIV (Synthetic Initialization Vector) encryption
- **Vendored Location**: `Vendored/Miscreant/`
- **Files Copied**:
  - Aead.cs
  - AesCmac.cs
  - AesCtr.cs
  - AesPmac.cs
  - AesSiv.cs
  - Constants.cs
  - IMac.cs
  - NonceEncoder.cs
  - StreamDecryptor.cs
  - StreamEncryptor.cs
  - Subtle.cs
  - Utils.cs
  - LICENSE.txt

- **Modifications**:
  - Changed namespace from `Miscreant` to `Confluent.SchemaRegistry.Encryption.Vendored.Miscreant`
  - Removed `AssemblyInfo.cs` (not needed)

### 2. HKDF.Standard (v2.0.0)
- **Original Repository**: https://github.com/andreimilto/HKDF.Standard
- **License**: MIT (see LICENSE)
- **Purpose**: HMAC-based Extract-and-Expand Key Derivation Function (HKDF)
- **Vendored Location**: `Vendored/HkdfStandard/`
- **Files Copied**:
  - Hkdf.cs (simplified implementation)
  - LICENSE

- **Modifications**:
  - Changed namespace from `HkdfStandard` to `Confluent.SchemaRegistry.Encryption.Vendored.HkdfStandard`
  - **Simplified implementation**: The original library had complex conditional compilation for different .NET versions and Span<T> support. Since the Confluent.SchemaRegistry.Encryption project only uses the `Hkdf.DeriveKey()` method, a simplified implementation was created that:
    - Implements only the `DeriveKey()` method
    - Works across all target frameworks (netstandard2.1, net462, net6.0, net8.0)
    - Uses standard byte[] arrays instead of Span<T> for compatibility
    - Maintains full RFC 5869 compliance for the DeriveKey operation

## Usage in Project

### Miscreant
Used in:
- `Cryptor.cs` - For AES-SIV encryption/decryption operations
- `KmsClients.cs` - For cryptographic operations

### HKDF.Standard
Used in:
- `LocalKmsClient.cs` - For key derivation from secrets

## Project Configuration

The `Confluent.SchemaRegistry.Encryption.csproj` file has been updated to:
1. Remove NuGet package references for `Miscreant` and `HKDF.Standard`
2. Automatically include all C# files in the `Vendored/` directory (via .NET SDK auto-inclusion)
3. Include license files in the NuGet package

## Attribution

Both libraries are used under the MIT License and are attributed to their original authors:
- **Miscreant**: Copyright (c) 2017-2018 The Miscreant Developers
- **HKDF.Standard**: Original implementation by andreimilto

## Maintenance Notes

- These vendored libraries should be updated periodically by pulling the latest source from their respective repositories
- When updating, ensure to:
  1. Update the namespace references
  2. Test compilation across all target frameworks
  3. Update version information in this README
  4. For HKDF, maintain the simplified implementation unless additional methods are needed

---
Last Updated: 2025-11-27

