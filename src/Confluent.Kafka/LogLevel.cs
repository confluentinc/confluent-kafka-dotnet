namespace Confluent.Kafka
{
    /// <summary>
    ///  Represents enumeration with levels comming from syslog(3)
    /// </summary>
    public enum LogLevel
    {
        /// <summary>
        ///  debug-level message
        /// </summary>
        Debug,
        /// <summary>
        ///  informational message
        /// </summary>
        Info,
        /// <summary>
        ///  normal, but significant condition
        /// </summary>
        Notice,
        /// <summary>
        ///  warning condition
        /// </summary>
        Warning,
        /// <summary>
        ///  error condition
        /// </summary>
        Error,
        /// <summary>
        ///  critical condition
        /// </summary>
        Critical,
        /// <summary>
        ///  action must be take immediately
        /// </summary>
        Alert,
        /// <summary>
        ///  system is unstable
        /// </summary>
        Emergency,
    }
}