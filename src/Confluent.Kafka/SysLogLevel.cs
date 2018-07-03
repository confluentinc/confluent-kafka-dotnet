namespace Confluent.Kafka
{
    /// <summary>
    ///     Represents enumeration with levels comming from syslog(3)
    /// </summary>
    public enum SyslogLevel
    {
        /// <summary>
        ///     System is unusable.
        /// </summary>
        Emergency = 0,

        /// <summary>
        ///     Action must be take immediately
        /// </summary>
        Alert = 1,

        /// <summary>
        ///     Critical condition.
        /// </summary>
        Critical = 2,

        /// <summary>
        ///     Error condition.
        /// </summary>
        Error = 3,

        /// <summary>
        ///     Warning condition.
        /// </summary>
        Warning = 4,

        /// <summary>
        ///     Normal, but significant condition.
        /// </summary>
        Notice = 5,
        
        /// <summary>
        ///     Informational message.
        /// </summary>
        Info = 6,
        
        /// <summary>
        ///     Debug-level message.
        /// </summary>
        Debug = 7
    }
}
