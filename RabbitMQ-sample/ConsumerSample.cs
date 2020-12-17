using LightMessager.Helper;
using LightMessager.Message;
using LightMessager.RabbitMQ_sample.MessageModel;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace LightMessager.RabbitMQ_sample
{
    public class ConsumerSample
    {
        /// <summary>
        /// 消费端-direct模式
        /// </summary>
        public void DirectConsumer()
        {
            RabbitMQHelper.RegisterHandler<NewOrderMessage, MQ_NewOrderHandler>();
        }
        /// <summary>
        /// topic模式
        /// </summary>
        public void TopicConsumer()
        {
            var routingKey = "com.mq.rabbit.*";//这里匹配路由键为com.mq.rabbit开头的消息
            RabbitMQHelper.RegisterHandlerAs<NewOrderMessage, MQ_NewOrderHandler>(routingKey);
        }
        /// <summary>
        /// 发布订阅模式，这里可添加多个订阅
        /// </summary>
        public void FanoutConsumer1()
        {
            RabbitMQHelper.RegisterHandlerForFanout<NewOrderMessage, MQ_NewOrderHandler>();
        }
        /// <summary>
        /// 这个同上一个都会收到订阅的消息，还可以添加更多
        /// </summary>
        public void FanoutConsumer2()
        {
            RabbitMQHelper.RegisterHandlerForFanout<NewOrderMessage, MQ_NewOrderHandler1>();
        }
    }
    /// <summary>
    /// 消息处理
    /// </summary>
    public class MQ_NewOrderHandler : BaseHandleMessages<NewOrderMessage>
    {
        protected override async Task DoHandle(NewOrderMessage message)
        {
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
            ///message logic 2
        }
    }
}
