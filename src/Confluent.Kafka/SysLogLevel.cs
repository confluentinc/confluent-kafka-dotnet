namespace Confluent.Kafka
{
    /// <summary>
    ///  Represents enumeration with levels comming from syslog(3)
    /// </summary>
    public enum SyslogLevel
    {
        /// <summary>
        ///  debug-level message
        /// </summary>
        Debug = 7,
        /// <summary>
        ///  informational message
        /// </summary>
        Info = 6,
        /// <summary>
        ///  normal, but significant condition
        /// </summary>
        Notice = 5,
        /// <summary>
        ///  warning condition
        /// </summary>
        Warning = 4,
        /// <summary>
        ///  error condition
        /// </summary>
        Error = 3,
        /// <summary>
        ///  critical condition
        /// </summary>
        Critical = 2,
        /// <summary>
        ///  action must be take immediately
        /// </summary>
        Alert = 1,
        /// <summary>
        ///  system is unstable
        /// </summary>
        Emergency = 0,
    }
}