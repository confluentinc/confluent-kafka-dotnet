using System;

namespace Confluent.Kafka
{
    public static class Loggers
    {
        public static void ConsoleLogger(object obj, LogMessage logInfo)
        {
            var now = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");
            Console.Error.WriteLine($"{logInfo.Level}|{now}|{logInfo.Name}|{logInfo.Facility}| {logInfo.Message}");
        }
    }
}
