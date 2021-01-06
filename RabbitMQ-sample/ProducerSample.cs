using LightMessager.Common;
using LightMessager.Helper;
using LightMessager.RabbitMQ_sample.MessageModel;
using System;
using System.Collections.Generic;
using System.Text;

namespace LightMessager.RabbitMQ_sample
{
    public class ProducerSample
    {
        private RabbitMQProducer rabbitMQProducer;
        public ProducerSample()
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
            rabbitMQProducer = new RabbitMQProducer(conn);
        }
        public void DirectSendMsg()
        {
            var send_msg = new NewOrderMessage
            {
                CreatedTime = DateTime.Now,
                OrderNum = "111111",
                PayType = 1,
                Price = 100M,
                ProductCode = 1,
                Source = Guid.NewGuid().ToString()
            };
            ///direct模式
            rabbitMQProducer.DirectSend(send_msg);
        }
        public void TopicSendMsg()
        {
            var send_msg = new NewOrderMessage
            {
                CreatedTime = DateTime.Now,
                OrderNum = "22222",
                PayType = 1,
                Price = 100M,
                ProductCode = 1,
                Source = Guid.NewGuid().ToString(),
                routeKey = "com.mq.rabbit.order"//这个根据自己定义，路由键，客户端消费需要订阅，匹配这个路由键才可以消费到。
        };
            rabbitMQProducer.TopicSend(send_msg);
        }
        public void FanoutSendMsg()
        {
            var send_msg = new NewOrderMessage
            {
                CreatedTime = DateTime.Now,
                OrderNum = "33333",
                PayType = 1,
                Price = 100M,
                ProductCode = 1,
                Source = Guid.NewGuid().ToString()
            };
            ///fanout模式
            rabbitMQProducer.FanoutSend(send_msg);
        }


        public void DirecSendMsgWihtExchange()
        {
            var send_msg = new NewOrderMessage
            {
                CreatedTime = DateTime.Now,
                OrderNum = "111111",
                PayType = 1,
                Price = 100M,
                ProductCode = 1,
                Source = Guid.NewGuid().ToString(),
                exchangeName = "directExchange",
                queueName = "directOrder",
                routeKey = "order1"
            };
            ///direct模式
            rabbitMQProducer.DirectSend(send_msg);
        }
        public void TopicSendMsgWihtExchange()
        {
            var send_msg = new NewOrderMessage
            {
                CreatedTime = DateTime.Now,
                OrderNum = "22222",
                PayType = 1,
                Price = 100M,
                ProductCode = 1,
                Source = Guid.NewGuid().ToString(),
                exchangeName = "topicExchange",
                queueName = "topicOrder",
                routeKey = "com.mq.rabbit.order"
            };
            ///Topic模式
            //var routingKey = "com.mq.rabbit.order";//这个根据自己定义，路由键，客户端消费需要订阅，匹配这个路由键才可以消费到。
            rabbitMQProducer.TopicSend(send_msg);
        }

        public void FanoutSendMsgWihtExchange()
        {
            var send_msg = new NewOrderMessage
            {
                CreatedTime = DateTime.Now,
                OrderNum = "33333",
                PayType = 1,
                Price = 100M,
                ProductCode = 1,
                Source = Guid.NewGuid().ToString(),
                exchangeName = "fanoutExchange"
            };
            ///fanout模式
            rabbitMQProducer.FanoutSend(send_msg);
        }
    }
}