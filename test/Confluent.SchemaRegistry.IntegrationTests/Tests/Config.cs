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
        public string EnableSslCertificateVerification { get; set; }
    }
}
