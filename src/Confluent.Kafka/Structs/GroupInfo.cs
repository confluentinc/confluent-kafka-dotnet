using System.Collections.Generic;


namespace Confluent.Kafka
{
    public struct GroupInfo
    {
        public BrokerMetadata Broker { get; set; } /**< Originating broker info */
        public string Group { get; set; }          /**< Group name */
        public ErrorCode Error { get; set; }       /**< Broker-originated error */
        public string State { get; set; }          /**< Group state */
        public string ProtocolType { get; set; }   /**< Group protocol type */
        public string Protocol { get; set; }       /**< Group protocol */
        public List<GroupMemberInfo> Members { get; set; } /**< Group members */
    }
}
