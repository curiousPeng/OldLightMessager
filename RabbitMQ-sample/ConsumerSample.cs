using LightMessager.Common;
using LightMessager.Helper;
using LightMessager.Message;
using LightMessager.RabbitMQ_sample.MessageModel;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace LightMessager.RabbitMQ_sample
{
    public class ConsumerSample
    {
        private RabbitMQConsumer rabbitMQConsumer;
        public ConsumerSample()
        {
            var conn = new ConnectionModel()
            {
                AutomaticRecoveryEnabled = true,
                HostName = "127.0.0.1",
                NetworkRecoveryInterval = TimeSpan.FromSeconds(15),
                Password = "123456",
                Port = 5672,
                UserName = "admin",
                VirtualHost = "/"
            };
            rabbitMQConsumer = new RabbitMQConsumer(conn);
        }
        /// <summary>
        /// 消费端-direct模式
        /// </summary>
        public void DirectConsumer()
        {
            rabbitMQConsumer.RegisterDirectHandler<NewOrderMessage, MQ_NewOrderHandler>();
        }
        /// <summary>
        /// 消费端-direct模式
        /// </summary>
        public void DirectConsumerWithExchange()
        {
            rabbitMQConsumer.RegisterDirectHandler<NewOrderMessage, MQ_NewOrderHandler>("directExchange", "directOrder", "order1");
        }
        /// <summary>
        /// topic模式
        /// </summary>
        public void TopicConsumer()
        {
            var routingKey = "com.mq.rabbit.*";//这里匹配路由键为com.mq.rabbit开头的消息
            rabbitMQConsumer.RegisterTopicHandler<NewOrderMessage, MQ_NewOrderHandler>(routingKey);
        }
        public void TopicConsumerWithExchange()
        {
            var routingKey = "com.mq.rabbit.*";//这里匹配路由键为com.mq.rabbit开头的消息
            rabbitMQConsumer.RegisterTopicHandler<NewOrderMessage, MQ_NewOrderHandler>(routingKey, "topicExchange", "topicOrder");
        }
        /// <summary>
        /// 发布订阅模式，这里可添加多个订阅
        /// </summary>
        public void FanoutConsumer1()
        {
            rabbitMQConsumer.RegisterFanoutHandler<NewOrderMessage, MQ_NewOrderHandler>();
        }
        /// <summary>
        /// 这个同上一个都会收到订阅的消息，还可以添加更多
        /// </summary>
        public void FanoutConsumer2()
        {
            rabbitMQConsumer.RegisterFanoutHandler<NewOrderMessage, MQ_NewOrderHandler1>();
        }
    }
    /// <summary>
    /// 消息处理
    /// </summary>
    public class MQ_NewOrderHandler : BaseHandleMessages<NewOrderMessage>
    {
        protected override async Task DoHandle(NewOrderMessage message)
        {
            Console.WriteLine(JsonConvert.SerializeObject(message));
            ///message logic
        }
    }

    /// <summary>
    /// fanout模式专用
    /// </summary>
    public class MQ_NewOrderHandler1 : BaseHandleMessages<NewOrderMessage>
    {
        protected override async Task DoHandle(NewOrderMessage message)
        {
            Console.WriteLine(JsonConvert.SerializeObject(message) + ",,,22222");
            ///message logic 2
        }
    }
}