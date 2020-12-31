using LightMessager.RabbitMQ_sample;
using System;

namespace fanout_sample_se
{
    class Program
    {
        static void Main(string[] args)
        {

            var consumer = new ConsumerSample();
            consumer.FanoutConsumer2();
            Console.WriteLine("fanout 2 启动");
            Console.ReadLine();
        }
    }
}
