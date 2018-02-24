using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Confluent.Kafka
{
    public interface IBufferPool
    {
        byte[] Rent(int minLength);
        void Return(byte[] buffer);
    }
}
