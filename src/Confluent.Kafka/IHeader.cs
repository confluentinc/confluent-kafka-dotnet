namespace Confluent.Kafka
{
    public interface IHeader
    {
        string Key { get; }
        
        T GetValue<T>();
    }
}
