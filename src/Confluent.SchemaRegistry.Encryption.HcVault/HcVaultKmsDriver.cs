using System;
using System.Collections.Generic;

namespace Confluent.SchemaRegistry.Encryption.HcVault
{
    public class HcVaultKmsDriver : IKmsDriver
    {
        public static void Register()
        {
            KmsRegistry.RegisterKmsDriver(new HcVaultKmsDriver());
        }
    
        public static readonly string Prefix = "hcvault://";
        public static readonly string TokenId = "token.id";
        public static readonly string Namespace = "namespace";
        
        public string GetKeyUrlPrefix()
        {
            return Prefix;
        }

        public IKmsClient NewKmsClient(IDictionary<string, string> config, string keyUrl)
        {
            config.TryGetValue(TokenId, out string tokenId);
            config.TryGetValue(Namespace, out string ns);
            return new HcVaultKmsClient(keyUrl, ns, tokenId);
        }
    }
}