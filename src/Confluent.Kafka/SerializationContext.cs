namespace Confluent.Kafka
{
    public class SerializationContext
    {
        public SerializationContext(MessageComponentType componentType, string name, string topic)
        {
            Type = componentType;
            Topic = topic;
            Name = name;
        }

        public string Topic { get; private set; }
        
        public MessageComponentType Type { get; private set; }

        public string Name { get; private set; }
    }
}
