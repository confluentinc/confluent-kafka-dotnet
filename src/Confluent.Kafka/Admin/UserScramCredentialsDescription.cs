using System.Collections.Generic;
namespace Confluent.Kafka.Admin
{
    /// <summary>
    ///     Specification of a new topic to be created via the CreateTopics
    ///     method. This class is used for the same purpose as NewTopic in
    ///     the Java API.
    /// </summary>
    public class UserScramCredentialsDescription
    {

        /// <summary>
        ///     Username
        /// </summary>
        public string User { get; set; }

        /// <summary>
        ///     ScramCredentialInfos of the User
        /// </summary>
        public List<ScramCredentialInfo> ScramCredentialInfos { get; set; }

        /// <summary>
        ///     User Level Error
        /// </summary>
        public Error Error {get;set;}

    }
}
