using LightMessager.RabbitMQ_sample;
using System;

namespace fanout_sample
{
    class Program
    {
        static void Main(string[] args)
        {
            var consumer = new ConsumerSample();
            consumer.FanoutConsumer1();
            Console.WriteLine("fanout 1 启动");
            Console.ReadLine();
        }
    }
}
