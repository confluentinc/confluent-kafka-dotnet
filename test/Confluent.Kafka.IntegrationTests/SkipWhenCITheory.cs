using System;
using Xunit;

namespace Confluent.Kafka.IntegrationTests;

public sealed class SkipWhenCITheory : TheoryAttribute
{
    private const string JenkinsBuildIdEnvVarName = "BUILD_ID";
    
    public SkipWhenCITheory(string reason)
    {
        Skip = Environment.GetEnvironmentVariables().Contains(JenkinsBuildIdEnvVarName)
            ? reason
            : null;
    }
}