using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace WebConsumer
{
    public class ConsumerExecutor : HostedServiceWrap
    {
        private WebConsumer _webConsumer { get; }

        public ConsumerExecutor(WebConsumer webConsumer)
        {
            _webConsumer = webConsumer;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                await _webConsumer.Start();
            }
        }
    }
}
