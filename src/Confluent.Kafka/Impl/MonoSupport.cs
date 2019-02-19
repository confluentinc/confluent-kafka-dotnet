using System;
using System.Linq;

namespace Confluent.Kafka.Impl
{
#if NET45 || NET46 || NET47
    internal static class MonoSupport
    {
        private static readonly Lazy<bool> HasMonoRuntime = new Lazy<bool>(() => Type.GetType("Mono.Runtime") != null);

        public static bool IsMonoRuntime => HasMonoRuntime.Value;

        public static bool IsPlatform(params PlatformID[] platform)
        {
            return platform.Any(x => x == Environment.OSVersion.Platform);
        }
    }
#endif
}
