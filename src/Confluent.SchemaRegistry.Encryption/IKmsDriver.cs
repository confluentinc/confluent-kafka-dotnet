using System.Collections.Generic;

namespace Confluent.SchemaRegistry.Encryption
{
    public interface IKmsDriver
    {
        string GetKeyUrlPrefix();

        IKmsClient NewKmsClient(IDictionary<string, string> config, string keyUrl);
    }
}