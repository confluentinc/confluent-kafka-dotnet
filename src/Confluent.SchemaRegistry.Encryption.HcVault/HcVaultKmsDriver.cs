using System;
using System.Collections.Generic;
using VaultSharp.V1.AuthMethods;
using VaultSharp.V1.AuthMethods.AppRole;
using VaultSharp.V1.AuthMethods.Token;

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
        public static readonly string AppRoleId = "app.role.id";
        public static readonly string AppRoleSecretId = "app.role.secret.id";

        public string GetKeyUrlPrefix()
        {
            return Prefix;
        }

        public IKmsClient NewKmsClient(IDictionary<string, string> config, string keyUrl)
        {
            config.TryGetValue(TokenId, out string tokenId);
            if (tokenId == null)
            {
                tokenId = Environment.GetEnvironmentVariable("VAULT_TOKEN");
            }
            config.TryGetValue(Namespace, out string ns);
            if (ns == null)
            {
                ns = Environment.GetEnvironmentVariable("VAULT_NAMESPACE");
            }
            config.TryGetValue(AppRoleId, out string appRoleId);
            if (appRoleId == null)
            {
                appRoleId = Environment.GetEnvironmentVariable("VAULT_APP_ROLE_ID");
            }
            config.TryGetValue(AppRoleSecretId, out string appRoleSecretId);
            if (appRoleSecretId == null)
            {
                appRoleSecretId = Environment.GetEnvironmentVariable("VAULT_APP_ROLE_SECRET_ID");
            }

            IAuthMethodInfo authMethod;
            if (appRoleId != null && appRoleSecretId != null)
            {
                authMethod = new AppRoleAuthMethodInfo(appRoleId, appRoleSecretId);
            }
            else if (tokenId != null)
            {
                authMethod = new TokenAuthMethodInfo(tokenId);
            }
            else
            {
                throw new ArgumentException($"Either {TokenId} or both {AppRoleId} and {AppRoleSecretId} " +
                                            $"must be provided in config or environment variables.");
            }

            return new HcVaultKmsClient(keyUrl, ns, authMethod);
        }
    }
}