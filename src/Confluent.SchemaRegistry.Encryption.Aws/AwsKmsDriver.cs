using System;
using System.Collections.Generic;
using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;

namespace Confluent.SchemaRegistry.Encryption.Aws
{
    public class AwsKmsDriver : IKmsDriver
    {
        public static void Register()
        {
            KmsRegistry.RegisterKmsDriver(new AwsKmsDriver());
        }

        public static readonly string Prefix = "aws-kms://";
        public static readonly string AccessKeyId = "access.key.id";
        public static readonly string SecretAccessKey = "secret.access.key";
        public static readonly string Profile = "profile";
        public static readonly string RoleArn = "role.arn";
        public static readonly string RoleSessionName = "role.session.name";
        public static readonly string RoleExternalId = "role.external.id";

        public string GetKeyUrlPrefix()
        {
            return Prefix;
        }

        public IKmsClient NewKmsClient(IDictionary<string, string> config, string keyUrl)
        {
            config.TryGetValue(RoleArn, out string roleArn);
            if (roleArn == null)
            {
                roleArn = Environment.GetEnvironmentVariable("AWS_ROLE_ARN");
            }
            config.TryGetValue(RoleSessionName, out string roleSessionName);
            if (roleSessionName == null)
            {
                roleSessionName = Environment.GetEnvironmentVariable("AWS_ROLE_SESSION_NAME");
            }
            config.TryGetValue(RoleExternalId, out string roleExternalId);
            if (roleExternalId == null)
            {
                roleExternalId = Environment.GetEnvironmentVariable("AWS_ROLE_EXTERNAL_ID");
            }
            string roleWebIdentityTokenFile =
                Environment.GetEnvironmentVariable("AWS_WEB_IDENTITY_TOKEN_FILE");
            AWSCredentials credentials = null;
            if (config.TryGetValue(AccessKeyId, out string accessKeyId) 
                && config.TryGetValue(SecretAccessKey, out string secretAccessKey))
            {
                credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);
            }
            else if (config.TryGetValue(Profile, out string profile))
            {
                var credentialProfileStoreChain = new CredentialProfileStoreChain();
                if (credentialProfileStoreChain.TryGetAWSCredentials(
                        profile, out AWSCredentials creds))
                    credentials = creds;
            }
            if (credentials == null)
            {
                credentials = FallbackCredentialsFactory.GetCredentials();
            }
            // If roleWebIdentityTokenFile is set, use the DefaultCredentialsProvider
            if (roleArn != null && roleWebIdentityTokenFile == null)
            {
                if (string.IsNullOrEmpty(roleExternalId))
                {
                    credentials = new AssumeRoleAWSCredentials(
                        credentials,
                        roleArn,
                        roleSessionName ?? "confluent-encrypt");
                }
                else
                {
                    var options = new AssumeRoleAWSCredentialsOptions
                    {
                        ExternalId = roleExternalId
                    };

                    credentials = new AssumeRoleAWSCredentials(
                        credentials,
                        roleArn,
                        roleSessionName ?? "confluent-encrypt",
                        options);
                }
            }
            return new AwsKmsClient(keyUrl, credentials);
        }
    }
}