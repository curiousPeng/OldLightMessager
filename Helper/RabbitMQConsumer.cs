using LightMessager.Common;
using LightMessager.Message;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using NLog;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;

namespace LightMessager.Helper
{
    /* 
     * links: 
     * https://www.rabbitmq.com/dotnet-api-guide.html
     * https://www.rabbitmq.com/queues.html
     * https://www.rabbitmq.com/confirms.html
     * https://stackoverflow.com/questions/4444208/delayed-message-in-rabbitmq
    */
    public sealed class RabbitMQConsumer : IRabbitMQConsumer
    {
        static ConnectionFactory factory;
        static IConnection connection;
        static readonly ushort prefetch_count;
        static Logger _logger = LogManager.GetLogger("RabbitMQHelper");

        public RabbitMQConsumer(IConfiguration configurationRoot)
        {
            factory = new ConnectionFactory();
            factory.UserName = configurationRoot.GetSection("LightMessager:UserName").Value; // "admin";
            factory.Password = configurationRoot.GetSection("LightMessager:Password").Value; // "123456";
            factory.VirtualHost = configurationRoot.GetSection("LightMessager:VirtualHost").Value; // "/";
            factory.HostName = configurationRoot.GetSection("LightMessager:HostName").Value; // "127.0.0.1";
            factory.Port = int.Parse(configurationRoot.GetSection("LightMessager:Port").Value); // 5672;
            factory.AutomaticRecoveryEnabled = true;
            factory.NetworkRecoveryInterval = TimeSpan.FromSeconds(15);
            connection = factory.CreateConnection();
        }
        public RabbitMQConsumer(ConnectionModel connectionModel)
        {
            factory = new ConnectionFactory();
            factory.UserName = connectionModel.UserName; // "admin";
            factory.Password = connectionModel.Password; // "123456";
            factory.VirtualHost = connectionModel.VirtualHost; // "/";
            factory.HostName = connectionModel.HostName; // "127.0.0.1";
            factory.Port = connectionModel.Port; // 5672;
            factory.AutomaticRecoveryEnabled = connectionModel.AutomaticRecoveryEnabled;//true
            factory.NetworkRecoveryInterval = connectionModel.NetworkRecoveryInterval;//15
            connection = factory.CreateConnection();
        }
        static RabbitMQConsumer()
        {
            prefetch_count = 100;
        }

        /// <summary>
        /// 注册消息处理器
        /// </summary>
        /// <typeparam name="TMessage">消息类型</typeparam>
        /// <typeparam name="THandler">消息处理器类型</typeparam>
        /// <param name="redeliveryCheck">是否开启重发确认；如果消息处理器逻辑已经实现为幂等则不需要开启以便节省计算资源，否则请打开该选项</param>
        public void RegisterDirectHandler<TMessage, THandler>(string exchangeName = "", string queueName = "", string routeKey = "")
            where THandler : BaseHandleMessages<TMessage>
            where TMessage : BaseMessage
        {
            try
            {
                var type = typeof(TMessage);
                var handler = Activator.CreateInstance<THandler>();
                var channel = connection.CreateModel();
                var consumer = new EventingBasicConsumer(channel);
                /*
                  @param prefetchSize maximum amount of content (measured in octets) that the server will deliver, 0 if unlimited
                  @param prefetchCount maximum number of messages that the server will deliver, 0 if unlimited
                  @param global true if the settings should be applied to the entire channel rather than each consumer
                */
                channel.BasicQos(0, prefetch_count, false);
                consumer.Received += async (model, ea) =>
                {
                    var json = Encoding.UTF8.GetString(ea.Body);
                    var msg = JsonConvert.DeserializeObject<TMessage>(json);
                    await handler.Handle(msg);
                    if (msg.NeedNAck)
                    {
                        channel.BasicNack(ea.DeliveryTag, false, true);
                    }
                    else
                    {
                        channel.BasicAck(ea.DeliveryTag, false);
                    }
                };

                var exchange = string.Empty;
                var queue = string.Empty;
                if (!string.IsNullOrEmpty(exchangeName) && !string.IsNullOrEmpty(queueName) && !string.IsNullOrEmpty(routeKey))
                {
                    exchange = exchangeName;
                    queue = queueName;
                    EnsureQueue.DirectEnsureQueue(channel, ref exchange, ref queue, routeKey);
                }
                else
                {
                    EnsureQueue.DirectEnsureQueue(channel, type, out exchange, out queue);
                }
                channel.BasicConsume(queue, false, consumer);
            }
            catch (Exception ex)
            {
                _logger.Debug("RegisterHandler()出错，异常：" + ex.Message + "；堆栈：" + ex.StackTrace);
            }
        }

        /// <summary>
        /// 注册消息处理器，根据模式匹配接收消息
        /// </summary>
        /// <typeparam name="TMessage">消息类型</typeparam>
        /// <typeparam name="THandler">消息处理器类型</typeparam>
        /// <param name="subscriberName">订阅器的名称</param>
        /// <param name="redeliveryCheck">是否开启重发确认；如果消息处理器逻辑已经实现为幂等则不需要开启以便节省计算资源，否则请打开该选项</param>
        /// <param name="subscribePatterns">订阅器支持的消息模式</param>
        public void RegisterTopicHandler<TMessage, THandler>(string routeKey, string exchangeName = "", string queueName = "")
            where THandler : BaseHandleMessages<TMessage>
            where TMessage : BaseMessage
        {
            if (string.IsNullOrWhiteSpace(routeKey))
            {
                throw new ArgumentNullException("routeKey");
            }
            try
            {
                var type = typeof(TMessage);
                var handler = Activator.CreateInstance<THandler>();
                var channel = connection.CreateModel();
                var consumer = new EventingBasicConsumer(channel);
                /*
                  @param prefetchSize maximum amount of content (measured in octets) that the server will deliver, 0 if unlimited
                  @param prefetchCount maximum number of messages that the server will deliver, 0 if unlimited
                  @param global true if the settings should be applied to the entire channel rather than each consumer
                */
                channel.BasicQos(0, prefetch_count, false);
                consumer.Received += async (model, ea) =>
                {
                    var json = Encoding.UTF8.GetString(ea.Body);
                    var msg = JsonConvert.DeserializeObject<TMessage>(json);
                    await handler.Handle(msg);
                    if (msg.NeedNAck)
                    {
                        channel.BasicNack(ea.DeliveryTag, false, true);
                    }
                    else
                    {
                        channel.BasicAck(ea.DeliveryTag, false);
                    }
                };

                var exchange = string.Empty;
                var queue = string.Empty;
                if (!string.IsNullOrEmpty(exchangeName) && !string.IsNullOrEmpty(queueName))
                {
                    exchange = exchangeName;
                    queue = queueName;
                    EnsureQueue.TopicEnsureQueue(channel, routeKey, ref exchange, ref queue);
                }
                else
                {
                    EnsureQueue.TopicEnsureQueue(channel, type, routeKey, out exchange, out queue);
                }
                channel.BasicConsume(queue, false, consumer);
            }
            catch (Exception ex)
            {
                _logger.Debug("RegisterHandler(string subscriberName)出错，异常：" + ex.Message + "；堆栈：" + ex.StackTrace);
            }
        }

        /// <summary>
        /// 注册消息处理器,fanout模式
        /// </summary>
        /// <typeparam name="TMessage">消息类型</typeparam>
        /// <typeparam name="THandler">消息处理器类型</typeparam>
        public void RegisterFanoutHandler<TMessage, THandler>(string exchangeName = "")
            where THandler : BaseHandleMessages<TMessage>
            where TMessage : BaseMessage
        {
            try
            {
                var type = typeof(TMessage);
                var handler = Activator.CreateInstance<THandler>();
                var channel = connection.CreateModel();
                var consumer = new EventingBasicConsumer(channel);

                var exchange = string.Empty;
                if (!string.IsNullOrEmpty(exchangeName))
                {
                    exchange = exchangeName;
                    EnsureQueue.FanoutEnsureQueue(channel, ref exchange);
                }
                else
                {
                    EnsureQueue.FanoutEnsureQueue(channel, type, out exchange);
                }
                var queueName = channel.QueueDeclare().QueueName;
                //将这个队列绑定（bind）到交换机上面
                channel.QueueBind(queue: queueName,exchange: exchange,routingKey: "");
                TMessage msg = null;//闭包获取数据
                consumer.Received += async (model, ea) =>
                {
                    var json = Encoding.UTF8.GetString(ea.Body);
                    msg = JsonConvert.DeserializeObject<TMessage>(json);
                    await handler.Handle(msg);
                };

                channel.BasicConsume(queueName, true, consumer);
            }
            catch (Exception ex)
            {
                _logger.Debug("RegisterHandler()出错，异常：" + ex.Message + "；堆栈：" + ex.StackTrace);
            }
        }
    }
}
