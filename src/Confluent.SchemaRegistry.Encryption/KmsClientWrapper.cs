using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Confluent.SchemaRegistry.Encryption
{
    public class KmsClientWrapper : IKmsClient
    {
        public IEnumerable<KeyValuePair<string, string>> Configs { get; }

        public RegisteredKek Kek { get; }

        public string KekId { get; }

        public IList<string> KmsKeyIds { get; }

        public KmsClientWrapper(IEnumerable<KeyValuePair<string, string>> configs, RegisteredKek kek)
        {
            Configs = configs;
            Kek = kek;
            KekId = kek.KmsType + EncryptionExecutor.KmsTypeSuffix + kek.KmsKeyId;
            KmsKeyIds = GetKmsKeyIds();
        }

        public bool DoesSupport(string uri)
        {
            return KekId == uri;
        }
        
        public async Task<byte[]> Encrypt(byte[] plaintext)
        {
            for (int i = 0; i < KmsKeyIds.Count; i++)
            {
                try
                {
                    IKmsClient kmsClient = GetKmsClient(Configs, Kek.KmsType, KmsKeyIds[i]);
                    return await kmsClient.Encrypt(plaintext).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    if (i == KmsKeyIds.Count - 1)
                    {
                        throw new RuleException("Failed to encrypt with all KEKs", e);
                    }
                }
            }
            return null;
        }

        public async Task<byte[]> Decrypt(byte[] ciphertext)
        {
            for (int i = 0; i < KmsKeyIds.Count; i++)
            {
                try
                {
                    IKmsClient kmsClient = GetKmsClient(Configs, Kek.KmsType, KmsKeyIds[i]);
                    return await kmsClient.Decrypt(ciphertext).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    if (i == KmsKeyIds.Count - 1)
                    {
                        throw new RuleException("Failed to decrypt with all KEKs", e);
                    }
                }
            }
            return null;
        }

        private IList<string> GetKmsKeyIds()
        {
            IList<string> kmsKeyIds = new List<string>();
            kmsKeyIds.Add(Kek.KmsKeyId);
            if (Kek.KmsProps != null)
            {
                if (Kek.KmsProps.TryGetValue(EncryptionExecutor.EncryptAlternateKmsKeyIds, out string alternateKmsKeyIds))
                {
                    char[] separators = { ',' };
                    string[] ids = alternateKmsKeyIds.Split(separators, StringSplitOptions.RemoveEmptyEntries);
                    foreach (string id in ids) {
                        if (!string.IsNullOrEmpty(id)) {
                            kmsKeyIds.Add(id);
                        }
                    }
                }
            }
            return kmsKeyIds;
        }

        private static IKmsClient GetKmsClient(IEnumerable<KeyValuePair<string, string>> configs, string kmsType, string kmsKeyId)
        {
            string keyUrl = kmsType + EncryptionExecutor.KmsTypeSuffix + kmsKeyId;
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
}