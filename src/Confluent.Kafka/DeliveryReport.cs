namespace Confluent.Kafka
{
    /*
    TODO: (via ewencp): The equivalent in the Python client fills in more information --
    the callbacks accept (err, msg) parameters, where the latter is
    http://docs.confluent.io/3.1.0/clients/confluent-kafka-python/index.html#confluent_kafka.Message
    Same deal with Go where the DR channel gets one of these:
    http://docs.confluent.io/3.1.0/clients/confluent-kafka-go/index.html#Message Is this being
    kept more minimal intentionally?
    */

    public struct DeliveryReport
    {
        public int Partition;
        public long Offset;
    }
}
