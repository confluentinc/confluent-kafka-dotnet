namespace Confluent.Kafka.Admin
{
    /// <summary>
    ///     Specification of a new topic to be created via the CreateTopics
    ///     method. This class is used for the same purpose as NewTopic in
    ///     the Java API.
    /// </summary>
    public class UserScramCredentialAlteration
    {
        /// <summary>
        ///     The username of the Alteration
        /// </summary>
        public string User { get; set; }

    }
}