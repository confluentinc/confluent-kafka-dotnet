using System.Collections.Generic;


namespace Confluent.Kafka
{
    public struct GroupInfo
    {
        public GroupInfo(BrokerMetadata broker, string grp, Error error, string state, string protocolType, string protocol, List<GroupMemberInfo> members)
        {
            Broker = broker;
            Group = grp;
            Error = error;
            State = state;
            ProtocolType = protocolType;
            Protocol = protocol;
            Members = members;
        }

        public BrokerMetadata Broker { get; } /**< Originating broker info */
        public string Group { get; }          /**< Group name */
        public Error Error { get; }           /**< Broker-originated error */
        public string State { get; }          /**< Group state */
        public string ProtocolType { get; }   /**< Group protocol type */
        public string Protocol { get; }       /**< Group protocol */
        public List<GroupMemberInfo> Members { get; } /**< Group members */
    }
}
