namespace Confluent.Kafka
{
    public interface IHeader
    {
        string Key { get; }
        
        byte[] GetValue();
        
        T GetValue<T>();
    }
}
