// Copyright 2016-2017 Confluent Inc.
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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;

namespace Confluent.Kafka.IntegrationTests
{
    public class EventObserverAndRecorder : IObserver<KeyValuePair<string, object>>, IDisposable
    {
        public static T ReadPublicProperty<T>(object obj, string propertyName)
        {
            Type type = obj.GetType();
            PropertyInfo property = type.GetProperty(propertyName, BindingFlags.Instance | BindingFlags.Public);
            return (T)property.GetValue(obj);
        }

        private class CallbackObserver<T> : IObserver<T>
        {
            public CallbackObserver(Action<T> callback) { _callback = callback; }
            public void OnCompleted() { }
            public void OnError(Exception error) { }
            public void OnNext(T value) { _callback(value); }

            private readonly Action<T> _callback;
        }

        private readonly IDisposable allListenersSubscription;
        private IDisposable listenerSubscription;
        private readonly Action<KeyValuePair<string, object>> onEvent;

        public EventObserverAndRecorder(string listenerName, Action<KeyValuePair<string, object>> onEvent = null)
        {
            allListenersSubscription = DiagnosticListener.AllListeners.Subscribe(
                new CallbackObserver<DiagnosticListener>(diagnosticListener =>
                {
                    if (diagnosticListener.Name == listenerName)
                    {
                        listenerSubscription = diagnosticListener.Subscribe(this);
                    }
                }));

            this.onEvent = onEvent;
        }

        public void Dispose()
        {
            allListenersSubscription.Dispose();
            listenerSubscription?.Dispose();
        }

        public ConcurrentQueue<KeyValuePair<string, object>> Records { get; } = new ConcurrentQueue<KeyValuePair<string, object>>();

        public void OnCompleted() { }
        public void OnError(Exception error) { }

        public void OnNext(KeyValuePair<string, object> record)
        {
            Records.Enqueue(record);
            onEvent?.Invoke(record);
        }
    }
}
