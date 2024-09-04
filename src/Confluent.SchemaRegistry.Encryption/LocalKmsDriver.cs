using System;
using System.Collections.Generic;

namespace Confluent.SchemaRegistry.Encryption
{
    
    public class LocalKmsDriver : IKmsDriver
    {
        public static void Register()
        {
            KmsRegistry.RegisterKmsDriver(new LocalKmsDriver());
        }
    
        public static readonly string Prefix = "local-kms://";
        public static readonly string Secret = "secret";
        
        public string GetKeyUrlPrefix()
        {
            return Prefix;
        }

        public IKmsClient NewKmsClient(IDictionary<string, string> config, string keyUrl)
        {
            config.TryGetValue(Secret, out string secret);
            return new LocalKmsClient(secret);
        }
    }
}