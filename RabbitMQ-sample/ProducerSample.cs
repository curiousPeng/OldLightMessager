﻿using LightMessager.Helper;
using LightMessager.RabbitMQ_sample.MessageModel;
using System;
using System.Collections.Generic;
using System.Text;

namespace LightMessager.RabbitMQ_sample
{
   public class ProducerSample
    {
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
            RabbitMQHelper.Send(send_msg);
        }

        public void TopicSendMsg()
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
            ///Topic模式
            var routingKey = "com.mq.rabbit.order";//这个根据自己定义，路由键，客户端消费需要订阅，匹配这个路由键才可以消费到。
            RabbitMQHelper.Publish(send_msg,routingKey);
        }
        public void FanoutSendMsg()
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
            ///fanout模式
            RabbitMQHelper.FanoutPublish(send_msg);
        }
    }
}
