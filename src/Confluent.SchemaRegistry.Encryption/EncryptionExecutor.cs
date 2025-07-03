// Copyright 2025 Confluent Inc.
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
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Confluent.SchemaRegistry.Encryption
{
    public class EncryptionExecutor : IRuleExecutor
    {
        public static void Register()
        {
            RuleRegistry.RegisterRuleExecutor(new EncryptionExecutor());
        }

        public static readonly string RuleType = "ENCRYPT_PAYLOAD";

        public static readonly string EncryptKekName = "encrypt.kek.name";
        public static readonly string EncryptKmsKeyid = "encrypt.kms.key.id";
        public static readonly string EncryptKmsType = "encrypt.kms.type";
        public static readonly string EncryptDekAlgorithm = "encrypt.dek.algorithm";
        public static readonly string EncryptDekExpiryDays = "encrypt.dek.expiry.days";

        public static readonly string KmsTypeSuffix = "://";

        internal static readonly int LatestVersion = -1;
        internal static readonly byte MagicByte = 0x0;
        internal static readonly int MillisInDay = 24 * 60 * 60 * 1000;
        internal static readonly int VersionSize = 4;

        internal IEnumerable<KeyValuePair<string, string>> Configs;
        internal IDekRegistryClient Client;
        internal IClock Clock;

        public EncryptionExecutor()
        {
            Clock = new Clock();
        }

        public EncryptionExecutor(IDekRegistryClient client, IClock clock)
        {
            Client = client;
            Clock = clock ?? new Clock();
        }

        public void Configure(IEnumerable<KeyValuePair<string, string>> config,
            ISchemaRegistryClient client = null)
        {
            if (Configs != null)
            {
                if (config != null && !new HashSet<KeyValuePair<string, string>>(Configs).SetEquals(config))
                {
                    throw new RuleException("EncryptionExecutor already configured");
                }
            }
            else
            {
                Configs = config ?? Array.Empty<KeyValuePair<string, string>>();
            }

            if (Client == null)
            {
                if (client != null)
                {
                    Client = new CachedDekRegistryClient(Configs, client.AuthHeaderProvider, client.Proxy);
                }
                else
                {
                    Client = new CachedDekRegistryClient(Configs);
                }
            }
        }

        public string Type() => RuleType;

        public async Task<object> Transform(RuleContext ctx, object message)
        {
            EncryptionExecutorTransform transform = NewTransform(ctx);
            var result = await transform.Transform(ctx, RuleContext.Type.Bytes, message)
                .ConfigureAwait(false);
            return result;
        }

        public EncryptionExecutorTransform NewTransform(RuleContext ctx)
        {
            EncryptionExecutorTransform transform = new EncryptionExecutorTransform(this);
            transform.Init(ctx);
            return transform;
        }

        internal Cryptor GetCryptor(RuleContext ctx)
        {
            string algorithm = ctx.GetParameter(EncryptDekAlgorithm);
            if (!Enum.TryParse<DekFormat>(algorithm, out DekFormat dekFormat))
            {
                dekFormat = DekFormat.AES256_GCM;
            }
            return new Cryptor(dekFormat);
        }

        internal static byte[] ToBytes(RuleContext.Type type, object obj)
        {
            switch (type)
            {
                case RuleContext.Type.Bytes:
                    if (obj is ReadOnlyMemory<byte> memory)
                    {
                        return memory.ToArray();
                    }
                    return (byte[])obj;
                case RuleContext.Type.String:
                    return Encoding.UTF8.GetBytes(obj.ToString()!);
                default:
                    return null;
            }
        }

        internal static object ToObject(RuleContext.Type type, byte[] bytes)
        {
            switch (type)
            {
                case RuleContext.Type.Bytes:
                    return bytes;
                case RuleContext.Type.String:
                    return Encoding.UTF8.GetString(bytes);
                default:
                    return null;
            }
        }
        
        public void Dispose()
        {
            if (Client != null)
            {
                Client.Dispose();
            }
        }
    }

    public class EncryptionExecutorTransform
    {

        private EncryptionExecutor executor;
        private Cryptor cryptor;
        private string kekName;
        private RegisteredKek registeredKek;
        private int dekExpiryDays;

        public EncryptionExecutorTransform(EncryptionExecutor executor)
        {
            this.executor = executor;
        }
        
        public void Init(RuleContext ctx)
        {
            cryptor = executor.GetCryptor(ctx);
            kekName = GetKekName(ctx);
            dekExpiryDays = GetDekExpiryDays(ctx);
        }

        public bool IsDekRotated() => dekExpiryDays > 0;

        private string GetKekName(RuleContext ctx)
        {
            string name = ctx.GetParameter(EncryptionExecutor.EncryptKekName);
            if (String.IsNullOrEmpty(name))
            {
                throw new RuleException("No kek name found");
            }

            return name;
        }

        private async Task<RegisteredKek> GetKek(RuleContext ctx)
        {
            if (registeredKek == null)
            {
                registeredKek = await GetOrCreateKek(ctx).ConfigureAwait(continueOnCapturedContext: false);
            }

            return registeredKek;
        }
        
        private async Task<RegisteredKek> GetOrCreateKek(RuleContext ctx)
        {
            bool isRead = ctx.RuleMode == RuleMode.Read;
            KekId kekId = new KekId(kekName, isRead);

            string kmsType = ctx.GetParameter(EncryptionExecutor.EncryptKmsType);
            string kmsKeyId = ctx.GetParameter(EncryptionExecutor.EncryptKmsKeyid);

            RegisteredKek kek = await RetrieveKekFromRegistry(kekId).ConfigureAwait(continueOnCapturedContext: false);
            if (kek == null)
            {
                if (isRead)
                {
                    throw new RuleException($"No kek found for name {kekName} during consume");
                }
                if (String.IsNullOrEmpty(kmsType))
                {
                    throw new RuleException($"No kms type found for {kekName} during produce");
                }
                if (String.IsNullOrEmpty(kmsKeyId))
                {
                    throw new RuleException($"No kms key id found for {kekName} during produce");
                }

                kek = await StoreKekToRegistry(kekId, kmsType, kmsKeyId, false)
                    .ConfigureAwait(continueOnCapturedContext: false);
                if (kek == null)
                {
                    // Handle conflicts (409)
                    kek = await RetrieveKekFromRegistry(kekId)
                        .ConfigureAwait(continueOnCapturedContext: false);
                }

                if (kek == null)
                {
                    throw new RuleException($"No kek found for {kekName} during produce");
                }
            }
            if (!String.IsNullOrEmpty(kmsType) && !kmsType.Equals(kek.KmsType))
            {
                throw new RuleException($"Found {kekName} with kms type {kek.KmsType} but expected {kmsType}");
            }
            if (!String.IsNullOrEmpty(kmsKeyId) && !kmsKeyId.Equals(kek.KmsKeyId))
            {
                throw new RuleException($"Found {kekName} with kms key id {kek.KmsKeyId} but expected {kmsKeyId}");
            }

            return kek;
        }
        
        private int GetDekExpiryDays(RuleContext ctx)
        {
            string expiryDays = ctx.GetParameter(EncryptionExecutor.EncryptDekExpiryDays);
            if (String.IsNullOrEmpty(expiryDays))
            {
                return 0;
            }
            if (!Int32.TryParse(expiryDays, out int days))
            {
                throw new RuleException($"Invalid expiry days {expiryDays}");
            }
            if (days < 0)
            {
                throw new RuleException($"Invalid expiry days {expiryDays}");
            }
            return days;
        }
        
        private async Task<RegisteredKek> RetrieveKekFromRegistry(KekId key)
        {
            if (executor.Client == null)
            {
                throw new RuleException("Pass a serializer/deserializer config to initialize the client");
            }
            try
            {
                return await executor.Client.GetKekAsync(key.Name, !key.LookupDeletedKeks)
                    .ConfigureAwait(continueOnCapturedContext: false);
            }
            catch (SchemaRegistryException e)
            {
                if (e.Status == HttpStatusCode.NotFound)
                {
                    return null;
                }

                throw new RuleException($"Failed to retrieve kek {key.Name}", e);
            }
        }
        
        private async Task<RegisteredKek> StoreKekToRegistry(KekId key, string kmsType, string kmsKeyId, bool shared)
        {
            if (executor.Client == null)
            {
                throw new RuleException("Pass a serializer/deserializer config to initialize the client");
            }
            Kek kek = new Kek
            {
                Name = key.Name,
                KmsType = kmsType,
                KmsKeyId = kmsKeyId,
                Shared = shared
            };
            try
            {
                return await executor.Client.CreateKekAsync(kek)
                    .ConfigureAwait(continueOnCapturedContext: false);
            }
            catch (SchemaRegistryException e)
            {
                if (e.Status == HttpStatusCode.Conflict)
                {
                    return null;
                }

                throw new RuleException($"Failed to create kek {key.Name}", e);
            }
        }
        
        private async Task<RegisteredDek> GetOrCreateDek(RuleContext ctx, int? version)
        {
            RegisteredKek kek = await GetKek(ctx).ConfigureAwait(continueOnCapturedContext: false);
            bool isRead = ctx.RuleMode == RuleMode.Read;
            DekId dekId = new DekId(kekName, ctx.Subject, version, cryptor.DekFormat, isRead);

            IKmsClient kmsClient = null;
            RegisteredDek dek = await RetrieveDekFromRegistry(dekId).ConfigureAwait(continueOnCapturedContext: false);
            bool isExpired = IsExpired(ctx, dek);
            if (dek == null || isExpired)
            {
                if (isRead)
                {
                    throw new RuleException($"No dek found for {kekName} during consume");
                }

                byte[] encryptedDek = null;
                if (!kek.Shared)
                {
                    kmsClient = GetKmsClient(executor.Configs, kek);
                    // Generate new dek
                    byte[] rawDek = cryptor.GenerateKey();
                    encryptedDek = await kmsClient.Encrypt(rawDek)
                        .ConfigureAwait(continueOnCapturedContext: false);
                }

                int? newVersion = isExpired ? dek.Version + 1 : null;
                try
                {
                    dek = await CreateDek(dekId, newVersion, encryptedDek)
                        .ConfigureAwait(continueOnCapturedContext: false);
                }
                catch (RuleException e)
                {
                    if (dek == null)
                    {
                        throw e;
                    }
                }
            }

            if (dek.KeyMaterialBytes == null)
            {
                if (kmsClient == null)
                {
                    kmsClient = GetKmsClient(executor.Configs, kek);
                }

                byte[] rawDek = await kmsClient.Decrypt(dek.EncryptedKeyMaterialBytes)
                    .ConfigureAwait(continueOnCapturedContext: false);
                dek.SetKeyMaterial(rawDek);

            }

            return dek;
        }

        private async Task<RegisteredDek> CreateDek(DekId dekId, int? newVersion, byte[] encryptedDek)
        {
            DekId newDekId = new DekId(dekId.KekName, dekId.Subject, newVersion, dekId.DekFormat, dekId.LookupDeletedDeks);
            RegisteredDek dek = await StoreDekToRegistry(newDekId, encryptedDek).ConfigureAwait(continueOnCapturedContext: false);
            if (dek == null)
            {
                // Handle conflicts (409)
                dek = await RetrieveDekFromRegistry(dekId).ConfigureAwait(continueOnCapturedContext: false);
            }

            if (dek == null)
            {
                throw new RuleException($"No dek found for {dekId.KekName} during produce");
            }

            return dek;
        }

        private bool IsExpired(RuleContext ctx, RegisteredDek dek)
        {
            long now = executor.Clock.NowToUnixTimeMilliseconds();
            return ctx.RuleMode != RuleMode.Read
                && dekExpiryDays > 0
                && dek != null
                && ((double) (now - dek.Timestamp)) / EncryptionExecutor.MillisInDay > dekExpiryDays;
        }
        
        private async Task<RegisteredDek> RetrieveDekFromRegistry(DekId key)
        {
            if (executor.Client == null)
            {
                throw new RuleException("Pass a serializer/deserializer config to initialize the client");
            }
            try
            {
                RegisteredDek dek;
                if (key.Version != null)
                {
                    dek = await executor.Client.GetDekVersionAsync(key.KekName, key.Subject, key.Version.Value, key.DekFormat,
                            !key.LookupDeletedDeks)
                        .ConfigureAwait(continueOnCapturedContext: false);

                }
                else
                {
                    dek = await executor.Client
                        .GetDekAsync(key.KekName, key.Subject, key.DekFormat, !key.LookupDeletedDeks)
                        .ConfigureAwait(continueOnCapturedContext: false);
                }

                return dek?.EncryptedKeyMaterial != null ? dek : null;
            }
            catch (SchemaRegistryException e)
            {
                if (e.Status == HttpStatusCode.NotFound)
                {
                    return null;
                }

                throw new RuleException($"Failed to retrieve dek for kek {key.KekName}, subject {key.Subject}", e);
            }
        }
        
        private async Task<RegisteredDek> StoreDekToRegistry(DekId key, byte[] encryptedDek)
        {
            if (executor.Client == null)
            {
                throw new RuleException("Pass a serializer/deserializer config to initialize the client");
            }
            string encryptedDekStr = encryptedDek != null ? Convert.ToBase64String(encryptedDek) : null;
            Dek dek = new Dek
            {
                Subject = key.Subject,
                Version = key.Version,
                Algorithm = key.DekFormat ?? DekFormat.AES256_GCM,
                EncryptedKeyMaterial = encryptedDekStr
            };
            try
            {
                return await executor.Client.CreateDekAsync(key.KekName, dek)
                    .ConfigureAwait(continueOnCapturedContext: false);
            }
            catch (SchemaRegistryException e)
            {
                if (e.Status == HttpStatusCode.Conflict)
                {
                    return null;
                }

                throw new RuleException($"Failed to create dek for kek {key.KekName}, subject {key.Subject}", e);
            }
        }

        public async Task<object> Transform(RuleContext ctx, RuleContext.Type type, object fieldValue)
        {
            if (fieldValue == null)
            {
                return null;
            }

            RegisteredDek dek;
            byte[] plaintext;
            byte[] ciphertext;
            switch (ctx.RuleMode)
            {
                case RuleMode.Write:
                    plaintext = EncryptionExecutor.ToBytes(type, fieldValue);
                    if (plaintext == null)
                    {
                        throw new RuleException($"Type {type} not supported for encryption");
                    }


                    dek = await GetOrCreateDek(ctx, IsDekRotated() ? EncryptionExecutor.LatestVersion : null)
                        .ConfigureAwait(continueOnCapturedContext: false);
                    ciphertext = cryptor.Encrypt(dek.KeyMaterialBytes, plaintext);
                    if (IsDekRotated())
                    {
                        ciphertext = PrefixVersion(dek.Version.Value, ciphertext);
                    }

                    if (type == RuleContext.Type.String)
                    {
                        return Convert.ToBase64String(ciphertext);
                    }
                    else
                    {
                        return EncryptionExecutor.ToObject(type, ciphertext);
                    }
                case RuleMode.Read:
                    if (type == RuleContext.Type.String)
                    {
                        ciphertext = Convert.FromBase64String((string)fieldValue);
                    }
                    else
                    {
                        ciphertext = EncryptionExecutor.ToBytes(type, fieldValue);
                    }

                    if (ciphertext == null)
                    {
                        return fieldValue;
                    }

                    int? version = null;
                    if (IsDekRotated())
                    {
                        (int, byte[]) kv = ExtractVersion(ciphertext);
                        version = kv.Item1;
                        ciphertext = kv.Item2;
                    }

                    dek = await GetOrCreateDek(ctx, version).ConfigureAwait(continueOnCapturedContext: false);
                    plaintext = cryptor.Decrypt(dek.KeyMaterialBytes, ciphertext);
                    return EncryptionExecutor.ToObject(type, plaintext);
                default:
                    throw new ArgumentException("Unsupported rule mode " + ctx.RuleMode);
            }
        }

        private byte[] PrefixVersion(int version, byte[] ciphertext)
        {
            byte[] buffer = new byte[1 + EncryptionExecutor.VersionSize + ciphertext.Length];
            using (MemoryStream stream = new MemoryStream(buffer))
            {
                using (BinaryWriter writer = new BinaryWriter(stream))
                {
                    writer.Write(EncryptionExecutor.MagicByte);
                    writer.Write(IPAddress.HostToNetworkOrder(version));
                    writer.Write(ciphertext);
                    return stream.ToArray();
                }
            }
        }

        private (int, byte[]) ExtractVersion(byte[] ciphertext)
        {
            using (MemoryStream stream = new MemoryStream(ciphertext))
            {
                using (BinaryReader reader = new BinaryReader(stream))
                {
                    int remainingSize = ciphertext.Length;
                    reader.ReadByte();
                    remainingSize--;
                    int version = IPAddress.NetworkToHostOrder(reader.ReadInt32());
                    remainingSize -= EncryptionExecutor.VersionSize;
                    byte[] remaining = reader.ReadBytes(remainingSize);
                    return (version, remaining);
                }
            }
        }
        
        private static IKmsClient GetKmsClient(IEnumerable<KeyValuePair<string, string>> configs, RegisteredKek kek)
        {
            string keyUrl = kek.KmsType + EncryptionExecutor.KmsTypeSuffix + kek.KmsKeyId;
            IKmsClient kmsClient = KmsRegistry.GetKmsClient(keyUrl);
            if (kmsClient == null)
            {
                IKmsDriver kmsDriver = KmsRegistry.GetKmsDriver(keyUrl);
                kmsClient = kmsDriver.NewKmsClient(
                    configs.ToDictionary(it => it.Key, it => it.Value), keyUrl);
                KmsRegistry.RegisterKmsClient(kmsClient);
            }

            return kmsClient;
        }
    }

    public interface IClock
    {
        long NowToUnixTimeMilliseconds();
    }

    internal class Clock : IClock
    {
        public long NowToUnixTimeMilliseconds() => DateTimeOffset.Now.ToUnixTimeMilliseconds();
    }
}