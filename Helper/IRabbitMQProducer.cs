using LightMessager.Message;
using System;
using System.Collections.Generic;
using System.Text;

namespace LightMessager.Helper
{
    public interface IRabbitMQProducer
    {
        bool DirectSend(BaseMessage message, string exchangeName, string queueName, string routeKey, int delaySend = 0);
        bool TopicSend(BaseMessage message, string exchangeName, string queueName, string routeKey, int delaySend = 0);
        bool FanoutSend(BaseMessage message, string exchangeName, int delaySend = 0);
    }
}
