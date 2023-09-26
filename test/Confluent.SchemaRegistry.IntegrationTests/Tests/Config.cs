namespace Confluent.SchemaRegistry.IntegrationTests
{
    public class Config
    {
        public string Server { get; set; }
        public string ServerWithAuth { get; set; }
        public string ServerWithSsl { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public string KeystoreLocation { get; set; }
        public string KeystorePassword { get; set; }
        public string CaLocation { get; set; }
        public string CaPem { get; set; }
        public string KeyEncryptedLocation { get; set; }
        public string KeyUnencryptedLocation { get; set; }
        public string KeyEncryptedPem { get; set; }
        public string KeyUnencryptedPem { get; set; }
        public string CertificatePem { get; set; }

        public string KeyPassword { get; set; }
        public string CertificateLocation { get; set; }
        public string EnableSslCertificateVerification { get; set; }
    }
}
