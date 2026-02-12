using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using Miscreant;

namespace Confluent.SchemaRegistry.Encryption
{
    public static class KmsClients
    {
        private static IDictionary<string, IKmsClient> clients = new ConcurrentDictionary<string, IKmsClient>();
        
        public static IKmsClient Get(string id)
        {
            return clients[id];
        }

        public static void Add(string id, IKmsClient kmsClient)
        {
            clients[id] = kmsClient;
        }
    }
}