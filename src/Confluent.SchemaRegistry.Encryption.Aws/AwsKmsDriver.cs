using System.Collections.Generic;
using Amazon.Runtime;

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
        
        public string GetKeyUrlPrefix()
        {
            return Prefix;
        }

        public IKmsClient NewKmsClient(IDictionary<string, string> config, string keyUrl)
        {
            AWSCredentials credentials = null;
            if (config.TryGetValue(AccessKeyId, out string accessKeyId) 
                && config.TryGetValue(SecretAccessKey, out string secretAccessKey))
            {
                credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);
            }
            return new AwsKmsClient(keyUrl, credentials);
        }
    }
}