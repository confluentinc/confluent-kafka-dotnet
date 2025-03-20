using System;

namespace Confluent.SchemaRegistry
{
    internal static class RetryUtility
    {
        private static readonly Random random = new Random();

        /// <summary>
        /// Calculates the retry delay using exponential backoff with jitter.
        /// </summary>
        /// <param name="baseDelayMs">Base delay in milliseconds</param>
        /// <param name="maxDelayMs">Maximum delay in milliseconds</param>
        /// <param name="retriesAttempted">Number of retries attempted so far</param>
        /// <returns>The calculated delay in milliseconds</returns>
        public static int CalculateRetryDelay(int baseDelayMs, int maxDelayMs, int retriesAttempted)
        {
            double jitter;
            lock (random) {
                jitter = random.NextDouble();
            }
            return Convert.ToInt32(Math.Min(jitter * Math.Pow(2, retriesAttempted) * baseDelayMs, maxDelayMs));
        }
    }
} 