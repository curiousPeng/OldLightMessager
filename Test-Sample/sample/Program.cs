using LightMessager.RabbitMQ_sample;
using System;

namespace sample
{
    class Program
    {
        static void Main(string[] args)
        {
            var producer = new ProducerSample();
            //producer.FanoutSendMsg();
            producer.TopicSendMsgWihtExchange();
            var consumer = new ConsumerSample();
            consumer.TopicConsumerWithExchange();
            Console.ReadLine();
        }
    }
}
