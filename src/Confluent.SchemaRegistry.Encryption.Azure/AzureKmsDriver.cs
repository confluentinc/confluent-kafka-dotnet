using System.Collections.Generic;
using Azure.Core;
using Azure.Identity;

namespace Confluent.SchemaRegistry.Encryption.Azure
{
    public class AzureKmsDriver : IKmsDriver
    {
        public static void Register()
        {
            KmsRegistry.RegisterKmsDriver(new AzureKmsDriver());
        }
    
        public static readonly string Prefix = "azure-kms://";
        public static readonly string TenantId = "tenant.id";
        public static readonly string ClientId = "client.id";
        public static readonly string ClientSecret = "client.secret";
        
        public string GetKeyUrlPrefix()
        {
            return Prefix;
        }

        public IKmsClient NewKmsClient(IDictionary<string, string> config, string keyUrl)
        {
            TokenCredential credential;
            if (config.TryGetValue(TenantId, out string tenantId) 
                && config.TryGetValue(ClientId, out string clientId)
                && config.TryGetValue(ClientSecret, out string clientSecret))
            {
                credential = new ClientSecretCredential(tenantId, clientId, clientSecret);
            }
            else
            {
                credential = new DefaultAzureCredential();
            }
            return new AzureKmsClient(keyUrl, credential);
        }
    }
}