using LightMessager.Message;
using System;
using System.Collections.Generic;
using System.Text;

namespace LightMessager.RabbitMQ_sample.MessageModel
{
    public class NewOrderMessage : BaseMessage
    {
        public string OrderNum;
        public decimal Price;
        public int ProductCode;
        public int PayType;
    }
}
