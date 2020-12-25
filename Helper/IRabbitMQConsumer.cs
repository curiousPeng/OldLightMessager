using LightMessager.Message;
using System;
using System.Collections.Generic;
using System.Text;

namespace LightMessager.Helper
{
    public interface IRabbitMQConsumer
    {
        void RegisterDirectHandler<TMessage, THandler>(string exchangeName, string queueName, string routeKey, bool IsNeedAck = true, bool redeliveryCheck = false) where THandler : BaseHandleMessages<TMessage> where TMessage : BaseMessage;
        void RegisterTopicHandler<TMessage, THandler>(string routeKey, string exchangeName, string queueName, bool IsNeedAck = true, bool redeliveryCheck = false, params string[] subscribePatterns) where THandler : BaseHandleMessages<TMessage> where TMessage : BaseMessage;
        void RegisterFanoutHandler<TMessage, THandler>(string exchangeName, string queueName, bool IsNeedAck = true, bool redeliveryCheck = false) where THandler : BaseHandleMessages<TMessage> where TMessage : BaseMessage;
    }
}
