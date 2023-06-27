namespace Confluent.Kafka.Admin
{
    /// <summary>
    ///     Specification of a new topic to be created via the CreateTopics
    ///     method. This class is used for the same purpose as NewTopic in
    ///     the Java API.
    /// </summary>
    public class UserScramCredentialUpsertion : UserScramCredentialAlteration
    {
        /// <summary>
        ///     The salt of the Upsertion
        /// </summary>
        public string Salt { get; set; }

        /// <summary>
        ///     The salt of the Upsertion
        /// </summary>
        public string Password { get; set; }

        /// <summary>
        ///     The salt of the Upsertion
        /// </summary>
        public ScramMechanism Mechanism { get; set; }

        /// <summary>
        ///     The salt of the Upsertion
        /// </summary>
        public int Iterations { get; set; }
    
    }
}



        
