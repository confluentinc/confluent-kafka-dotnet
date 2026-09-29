using System;
using System.Collections.Generic;
using System.IO;
using Aliyun.Credentials.Provider;
using AliCloudCredential = Aliyun.Credentials.Client;
using AliCloudCredentialConfig = Aliyun.Credentials.Models.Config;

namespace Confluent.SchemaRegistry.Encryption.AliCloud
{
    public class AliCloudKmsDriver : IKmsDriver
    {
        public static void Register()
        {
            KmsRegistry.RegisterKmsDriver(new AliCloudKmsDriver());
        }

        internal static readonly string Prefix = "alicloud-kms://";
        private static readonly string AccessKeyId = "access.key.id";
        private static readonly string AccessKeySecret = "access.key.secret";
        private static readonly string SecurityToken = "security.token";
        private static readonly string RoleArn = "role.arn";
        private static readonly string RoleSessionName = "role.session.name";
        private static readonly string RoleSessionExpiration = "role.session.expiration";
        private static readonly string Policy = "policy";
        private static readonly string StsEndpoint = "sts.endpoint";
        private static readonly string RoleExternalId = "role.external.id";
        private static readonly string Endpoint = "endpoint";
        private static readonly string CaFile = "ca.file";

        public string GetKeyUrlPrefix() => Prefix;

        public IKmsClient NewKmsClient(IDictionary<string, string> config, string keyUrl)
        {
            config.TryGetValue(AccessKeyId, out string accessKeyId);
            if (accessKeyId == null)
            {
                accessKeyId = Environment.GetEnvironmentVariable("ALIBABA_CLOUD_ACCESS_KEY_ID");
            }
            config.TryGetValue(AccessKeySecret, out string accessKeySecret);
            if (accessKeySecret == null)
            {
                accessKeySecret = Environment.GetEnvironmentVariable("ALIBABA_CLOUD_ACCESS_KEY_SECRET");
            }
            config.TryGetValue(SecurityToken, out string securityToken);
            if (securityToken == null)
            {
                securityToken = Environment.GetEnvironmentVariable("ALIBABA_CLOUD_SECURITY_TOKEN");
            }
            config.TryGetValue(RoleArn, out string roleArn);
            if (roleArn == null)
            {
                roleArn = Environment.GetEnvironmentVariable("ALIBABA_CLOUD_ROLE_ARN");
            }
            config.TryGetValue(RoleSessionName, out string roleSessionName);
            if (roleSessionName == null)
            {
                roleSessionName = Environment.GetEnvironmentVariable("ALIBABA_CLOUD_ROLE_SESSION_NAME");
            }
            config.TryGetValue(RoleSessionExpiration, out string roleSessionExpirationValue);
            int? roleSessionExpiration = null;
            if (!string.IsNullOrEmpty(roleSessionExpirationValue))
            {
                if (!int.TryParse(roleSessionExpirationValue, out var parsed))
                {
                    throw new ArgumentException(
                        $"Invalid {RoleSessionExpiration} value: '{roleSessionExpirationValue}' (expected integer seconds)");
                }
                roleSessionExpiration = parsed;
            }
            config.TryGetValue(Policy, out string policy);
            config.TryGetValue(StsEndpoint, out string stsEndpoint);
            config.TryGetValue(RoleExternalId, out string externalId);
            config.TryGetValue(Endpoint, out string endpoint);
            config.TryGetValue(CaFile, out string caFile);
            string caCert = caFile != null ? File.ReadAllText(caFile) : null;

            AliCloudCredential credential = NewCredential(
                accessKeyId, accessKeySecret, securityToken, roleArn, roleSessionName,
                roleSessionExpiration, policy, stsEndpoint, externalId);

            return new AliCloudKmsClient(keyUrl, credential, endpoint, caCert);
        }

        private static AliCloudCredential NewCredential(
            string accessKeyId, string accessKeySecret, string securityToken, string roleArn,
            string roleSessionName, int? roleSessionExpiration, string policy, string stsEndpoint,
            string externalId)
        {
            if (roleArn != null)
            {
                var builder = new RamRoleArnCredentialProvider.Builder()
                    .RoleArn(roleArn)
                    .RoleSessionName(string.IsNullOrEmpty(roleSessionName) ? "confluent-encrypt" : roleSessionName)
                    .Policy(policy)
                    .STSEndpoint(stsEndpoint)
                    .ExternalId(externalId);
                if (roleSessionExpiration.HasValue)
                {
                    builder.DurationSeconds(roleSessionExpiration.Value);
                }
                if (accessKeyId != null && accessKeySecret != null)
                {
                    builder.AccessKeyId(accessKeyId).AccessKeySecret(accessKeySecret).SecurityToken(securityToken);
                }
                else
                {
                    builder.CredentialsProvider(new DefaultCredentialsProvider());
                }
                return new AliCloudCredential(builder.Build());
            }

            if (accessKeyId != null && accessKeySecret != null)
            {
                var credentialConfig = new AliCloudCredentialConfig
                {
                    Type = securityToken != null ? "sts" : "access_key",
                    AccessKeyId = accessKeyId,
                    AccessKeySecret = accessKeySecret,
                    SecurityToken = securityToken,
                };
                return new AliCloudCredential(credentialConfig);
            }

            // Falls back to the default Alibaba Cloud credentials provider chain
            // (env vars, CLI/instance profile, ECS/ECI metadata, OIDC, etc.)
            return new AliCloudCredential();
        }
    }
}
