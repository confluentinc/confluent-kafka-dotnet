// Copyright 2022 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.

using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;

namespace Confluent.Kafka.IntegrationTests
{
    internal class ActivityEventsRecorder
    {
        internal ConcurrentQueue<KeyValuePair<string, IEnumerable<KeyValuePair<string, string>>>> Events = new();
        private readonly string activityName;

        internal ActivityEventsRecorder(string activityName)
        {
            this.activityName = activityName;
        }

        /// <summary>
        /// Builds an ActivityListener with callbacks to store start and stop events to a concurrent queue.
        /// </summary>
        /// <returns></returns>
        internal ActivityListener BuildActivityListener()
        {
            using var listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
                ActivityStarted = activity =>
                {
                    if (activity.DisplayName == activityName)
                        Events.Enqueue(new KeyValuePair<string, IEnumerable<KeyValuePair<string, string>>>(activity.Id, activity.Tags));
                },
                ActivityStopped = activity =>
                {
                    if (activity.DisplayName == activityName)
                        Events.Enqueue(new KeyValuePair<string, IEnumerable<KeyValuePair<string, string>>>(activity.Id, activity.Tags));
                }
            };

            return listener;
        }
    }
}
