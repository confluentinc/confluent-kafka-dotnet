using System;

namespace Confluent.Kafka.IntegrationTests
{
    [AttributeUsage(AttributeTargets.Method, Inherited = false)]
    public class IntegrationTestAttribute : Attribute
    {
        public IntegrationTestAttribute(bool explicitExecutionRequired = false)
        {
            ExplicitExecutionRequired = explicitExecutionRequired;
        }

        public bool ExplicitExecutionRequired { get; private set; }
    }
}
