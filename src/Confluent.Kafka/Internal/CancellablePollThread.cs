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
using System.Threading;
using System.Threading.Tasks;


namespace Confluent.Kafka
{
    internal class CancellablePollThread
    {
        private readonly int millisecondsTimeout;
        private readonly Object startStopLock = new Object();

        private CancellationTokenSource consumerCts = null;
        private Task consumerTask = null;

        public CancellablePollThread(int millisecondsTimeout = 100)
        {
            this.millisecondsTimeout = millisecondsTimeout;
        }

        public void Start(Action<int> pollMethod, Action<AggregateException> exceptionHandler)
        {
            lock (startStopLock)
            {
                if (consumerCts != null)
                {
                    throw new InvalidOperationException("Poll loop cannot be started twice.");
                }

                consumerCts = new CancellationTokenSource();
                var ct = consumerCts.Token;
                consumerTask = Task.Run(() =>
                    {
                        while (!ct.IsCancellationRequested)
                        {
                            pollMethod(millisecondsTimeout);
                        }
                    },
                    ct
                ).ContinueWith(t =>
                    {
                        lock (startStopLock)
                        {
                            consumerCts.Dispose();
                            consumerCts = null;
                            consumerTask = null;
                        }
                        exceptionHandler(t.Exception);
                    },
                    TaskContinuationOptions.OnlyOnFaulted
                );
            }
        }

        public bool IsStarted
            => consumerTask != null;

        public void Stop(bool throwIfNotStarted)
        {
            lock (startStopLock)
            {
                if (consumerCts == null)
                {
                    if (throwIfNotStarted)
                    {
                        throw new InvalidOperationException("Poll loop not started - cannot stop.");
                    }
                    else
                    {
                        return;
                    }
                }

                consumerCts.Cancel();

                try
                {
                    consumerTask.Wait();
                }
                catch (AggregateException ae)
                {
                    foreach (var e in ae.InnerExceptions)
                    {
                        if (e is TaskCanceledException)
                        {
                            continue;
                        }
                        throw ae;
                    }
                }

                consumerCts.Dispose();
                consumerCts = null;
                consumerTask = null;
            }
        }
    }
}
