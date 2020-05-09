// Copyright 2016-2018 Confluent Inc.
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

using System;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;

namespace Confluent.Kafka
{
    internal static class Diagnostics
    {
        public const string DiagnosticListenerName = "Confluent.Kafka.Listener";
        public const string TraceParentHeaderName = "traceparent";
        public const string TraceStateHeaderName = "tracestate";

        public static DiagnosticListener DiagnosticSource { get; } = new DiagnosticListener(DiagnosticListenerName);

        public static class Producer
        {
            public const string ActivityName = DiagnosticListenerName + ".MessageProduced";
            public const string StartName = ActivityName + ".Start";
            public const string StopName = ActivityName + ".Stop";
            public const string ExceptionName = ActivityName + ".Exception";

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static Activity Start<TKey, TValue>(string topic, Message<TKey, TValue> message)
            {
                if (DiagnosticSource.IsEnabled(ActivityName, topic))
                {
                    Activity activity = new Activity(ActivityName);
                    activity.Start();

                    AddActivityStateToHeaders(activity, message.Headers);

                    if (DiagnosticSource.IsEnabled(StartName))
                    {
                        DiagnosticSource.Write(StartName, new { Topic = topic, Message = message });
                    }

                    return activity;
                }

                return null;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static void Stop<TKey, TValue>(string topic, DeliveryResult<TKey, TValue> deliveryResult)
            {
                if (DiagnosticSource.IsEnabled(StopName))
                {
                    DiagnosticSource.Write(StopName, new { Topic = topic, DeliveryResult = deliveryResult });
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static void Exception<TKey, TValue>(string topic, Message<TKey, TValue> message, Exception exception)
            {
                if (DiagnosticSource.IsEnabled(ExceptionName))
                {
                    DiagnosticSource.Write(ExceptionName, new { Topic = topic, Message = message, Exception = exception });
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static void StopOrException<TKey, TValue>(string topic, DeliveryReport<TKey, TValue> deliveryReport)
            {
                if (deliveryReport.Error.IsError)
                {
                    if (DiagnosticSource.IsEnabled(ExceptionName))
                    {
                        DiagnosticSource.Write(
                            ExceptionName,
                            new
                            {
                                Topic = topic,
                                deliveryReport.Message,
                                Exception = new ProduceException<TKey, TValue>(deliveryReport.Error, deliveryReport)
                            });
                    }
                }
                else if (DiagnosticSource.IsEnabled(StopName))
                {
                    DiagnosticSource.Write(StopName, new { Topic = topic, DeliveryResult = deliveryReport });
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static void AddActivityStateToHeaders(Activity activity, Headers headers)
            {
                if (headers?.Any(h => h.Key == TraceParentHeaderName) != true)
                {
                    headers.Add(TraceParentHeaderName, Encoding.UTF8.GetBytes(activity.Id));

                    string traceState = activity.TraceStateString;
                    if (traceState != null)
                    {
                        headers.Add(TraceStateHeaderName, Encoding.UTF8.GetBytes(traceState));
                    }
                }
            }
        }
    }
}
