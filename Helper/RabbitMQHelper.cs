using LightMessager.Common;
using LightMessager.DAL;
using LightMessager.Message;
using LightMessager.Pool;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using NLog;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace LightMessager.Helper
{
    /* 
     * links: 
     * https://www.rabbitmq.com/dotnet-api-guide.html
     * https://www.rabbitmq.com/queues.html
     * https://www.rabbitmq.com/confirms.html
     * https://stackoverflow.com/questions/4444208/delayed-message-in-rabbitmq
    */
    public sealed class RabbitMQHelper
    {
        static ConnectionFactory factory;
        static IConnection connection;
        static volatile int prepersist_count;
        static readonly int default_retry_wait;
        static readonly int default_retry_count;
        static List<long> prepersist;
        static ConcurrentQueue<BaseMessage> retry_send_queue;
        static ConcurrentQueue<BaseMessage> retry_pub_queue;
        static ConcurrentQueue<BaseMessage> retry_fanout_pub_queue;
        static ConcurrentDictionary<Type, QueueInfo> dict_info;
        static ConcurrentDictionary<Type, object> dict_func;
        static ConcurrentDictionary<(Type, string), QueueInfo> dict_info_name; // with name
        static ConcurrentDictionary<(Type, string), object> dict_func_name; // with name
        static ConcurrentDictionary<Type, string> dict_info_fanout;
        static ConcurrentDictionary<Type, bool> dict_func_fanout;//Type是THandler，和上面的TMessage 不同，只限制一个处理订阅一次，不限制消息多次订阅
        static ConcurrentDictionary<Type, ObjectPool<IPooledWapper>> pools;
        static readonly ushort prefetch_count;
        static object lockobj = new object();
        static Logger _logger = LogManager.GetLogger("RabbitMQHelper");

        private RabbitMQHelper()
        { }

        static RabbitMQHelper()
        {
            #region 读取配置
            // 添加json配置文件路径
#if LOCAL
            var builder = new ConfigurationBuilder().SetBasePath(AppDomain.CurrentDomain.BaseDirectory).AddJsonFile("appsettings.Local.json");
#elif DEBUG
            var builder = new ConfigurationBuilder().SetBasePath(AppDomain.CurrentDomain.BaseDirectory).AddJsonFile("appsettings.Development.json");
#else
            var builder = new ConfigurationBuilder().SetBasePath(AppDomain.CurrentDomain.BaseDirectory).AddJsonFile("appsettings.json");
#endif
            // 创建配置根对象
            var configurationRoot = builder.Build();
            #endregion

            prefetch_count = 100;
            prepersist_count = 0;
            default_retry_wait = 1000; // 1秒
            default_retry_count = 3; // 消息重试最大3次
            prepersist = new List<long>();
            retry_send_queue = new ConcurrentQueue<BaseMessage>();
            retry_pub_queue = new ConcurrentQueue<BaseMessage>();
            retry_fanout_pub_queue = new ConcurrentQueue<BaseMessage>();
            dict_info = new ConcurrentDictionary<Type, QueueInfo>();
            dict_func = new ConcurrentDictionary<Type, object>();
            dict_info_name = new ConcurrentDictionary<(Type, string), QueueInfo>();
            dict_func_name = new ConcurrentDictionary<(Type, string), object>();
            dict_info_fanout = new ConcurrentDictionary<Type, string>();
            dict_func_fanout = new ConcurrentDictionary<Type, bool>();
            pools = new ConcurrentDictionary<Type, ObjectPool<IPooledWapper>>();
            factory = new ConnectionFactory();
            factory.UserName = configurationRoot.GetSection("LightMessager:UserName").Value; // "admin";
            factory.Password = configurationRoot.GetSection("LightMessager:Password").Value; // "123456";
            factory.VirtualHost = configurationRoot.GetSection("LightMessager:VirtualHost").Value; // "/";
            factory.HostName = configurationRoot.GetSection("LightMessager:HostName").Value; // "127.0.0.1";
            factory.Port = int.Parse(configurationRoot.GetSection("LightMessager:Port").Value); // 5672;
            factory.AutomaticRecoveryEnabled = true;
            factory.NetworkRecoveryInterval = TimeSpan.FromSeconds(15);
            connection = factory.CreateConnection();

            // 开启轮询检测，扫描重试队列，重发消息
            new Thread(() =>
            {
                // 先实现为spin的方式，后面考虑换成blockingqueue的方式
                while (true)
                {
                    BaseMessage send_item;
                    while (retry_send_queue.TryDequeue(out send_item))
                    {
                        Send(send_item);
                    }

                    BaseMessage pub_item;
                    while (retry_pub_queue.TryDequeue(out pub_item))
                    {
                        Publish(pub_item, pub_item.Pattern);
                    }

                    BaseMessage pub_item_fanout;
                    while (retry_fanout_pub_queue.TryDequeue(out pub_item_fanout))
                    {
                        FanoutPublish(pub_item_fanout);
                    }
                    Thread.Sleep(1000 * 5);
                }
            }).Start();
        }

        /// <summary>
        /// 注册消息处理器
        /// </summary>
        /// <typeparam name="TMessage">消息类型</typeparam>
        /// <typeparam name="THandler">消息处理器类型</typeparam>
        /// <param name="redeliveryCheck">是否开启重发确认；如果消息处理器逻辑已经实现为幂等则不需要开启以便节省计算资源，否则请打开该选项</param>
        public static void RegisterHandler<TMessage, THandler>(bool redeliveryCheck = false)
            where THandler : BaseHandleMessages<TMessage>
            where TMessage : BaseMessage
        {
            try
            {
                var type = typeof(TMessage);
                if (!dict_func.ContainsKey(type))
                {
                    var handler = dict_func.GetOrAdd(type, t => Activator.CreateInstance<THandler>()) as THandler;
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
                        if (redeliveryCheck)
                        {
                            if (!ea.Redelivered) // 之前一定没有处理过该条消息
                            {
                                await handler.Handle(msg);
                                if (msg.NeedNAck)
                                {
                                    channel.BasicNack(ea.DeliveryTag, false, true);
                                    MessageQueueHelper.Update(
                                    msg.MsgHash,
                                    fromStatus: MsgStatus.ArrivedBroker,
                                    toStatus: MsgStatus.Exception);
                                }
                                else
                                {
                                    channel.BasicAck(ea.DeliveryTag, false);
                                    MessageQueueHelper.Update(
                                    msg.MsgHash,
                                    fromStatus: MsgStatus.ArrivedBroker,
                                    toStatus: MsgStatus.Consumed);
                                }
                            }
                            else
                            {
                                var m = MessageQueueHelper.GetModelBy(msg.MsgHash);
                                if (m.Status == MsgStatus.Exception)
                                {
                                    await handler.Handle(msg);
                                    if (msg.NeedNAck)
                                    {
                                        channel.BasicNack(ea.DeliveryTag, false, true);
                                    }
                                    else
                                    {
                                        channel.BasicAck(ea.DeliveryTag, false);
                                    }
                                }
                                else if (m.Status == MsgStatus.ArrivedBroker)
                                {
                                    // 相对特殊的一种情况，Redelivered为true，但是本地消息实际上只到达第三档状态
                                    // 说明在消息刚从broker出来，rabbitmq重置了链接
                                    await handler.Handle(msg);
                                    if (msg.NeedNAck)
                                    {
                                        channel.BasicNack(ea.DeliveryTag, false, true);
                                    }
                                    else
                                    {
                                        channel.BasicAck(ea.DeliveryTag, false);
                                    }
                                }
                                else
                                {
                                    // 为了保持幂等这里不做任何处理
                                }
                            }
                        }
                        else
                        {
                            await handler.Handle(msg);
                            if (msg.NeedNAck)
                            {
                                channel.BasicNack(ea.DeliveryTag, false, true);
                            }
                            else
                            {
                                channel.BasicAck(ea.DeliveryTag, false);
                            }
                        }
                    };

                    var exchange = string.Empty;
                    var route_key = string.Empty;
                    var queue = string.Empty;
                    EnsureQueue(channel, type, out exchange, out route_key, out queue);
                    channel.BasicConsume(queue, false, consumer);
                }
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
        public static void RegisterHandlerAs<TMessage, THandler>(string subscriberName, bool redeliveryCheck = false, params string[] subscribePatterns)
            where THandler : BaseHandleMessages<TMessage>
            where TMessage : BaseMessage
        {
            if (string.IsNullOrWhiteSpace(subscriberName))
            {
                throw new ArgumentNullException("subscriberName");
            }

            if (subscribePatterns == null || subscribePatterns.Length == 0)
            {
                throw new ArgumentNullException("subscribePatterns");
            }

            try
            {
                var key = (typeof(TMessage), subscriberName);
                if (!dict_func_name.ContainsKey(key))
                {
                    var handler = dict_func_name.GetOrAdd((typeof(TMessage), subscriberName), p => Activator.CreateInstance<THandler>()) as THandler;
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
                        if (redeliveryCheck)
                        {
                            if (!ea.Redelivered) // 之前一定没有处理过该条消息
                            {
                                await handler.Handle(msg);
                                if (msg.NeedNAck)
                                {
                                    channel.BasicNack(ea.DeliveryTag, false, true);
                                    MessageQueueHelper.Update(
                                    msg.MsgHash,
                                    fromStatus: MsgStatus.ArrivedBroker,
                                    toStatus: MsgStatus.Exception);
                                }
                                else
                                {
                                    channel.BasicAck(ea.DeliveryTag, false);
                                    MessageQueueHelper.Update(
                                    msg.MsgHash,
                                    fromStatus: MsgStatus.ArrivedBroker,
                                    toStatus: MsgStatus.Consumed);
                                }
                            }
                            else
                            {
                                var m = MessageQueueHelper.GetModelBy(msg.MsgHash);
                                if (m.Status == MsgStatus.Exception)
                                {
                                    await handler.Handle(msg);
                                    if (msg.NeedNAck)
                                    {
                                        channel.BasicNack(ea.DeliveryTag, false, true);
                                    }
                                    else
                                    {
                                        channel.BasicAck(ea.DeliveryTag, false);
                                    }
                                }
                                else if (m.Status == MsgStatus.ArrivedBroker)
                                {
                                    // 相对特殊的一种情况，Redelivered为true，但是本地消息实际上只到达第三档状态
                                    // 说明在消息刚从broker出来，rabbitmq重置了链接
                                    await handler.Handle(msg);
                                    if (msg.NeedNAck)
                                    {
                                        channel.BasicNack(ea.DeliveryTag, false, true);
                                    }
                                    else
                                    {
                                        channel.BasicAck(ea.DeliveryTag, false);
                                    }
                                }
                                else
                                {
                                    // 为了保持幂等这里不做任何处理
                                }
                            }
                        }
                        else
                        {
                            await handler.Handle(msg);
                            if (msg.NeedNAck)
                            {
                                channel.BasicNack(ea.DeliveryTag, false, true);
                            }
                            else
                            {
                                channel.BasicAck(ea.DeliveryTag, false);
                            }
                        }
                    };

                    var exchange = string.Empty;
                    var queue = string.Empty;
                    EnsureQueue(channel, typeof(TMessage), subscriberName, out exchange, out queue, subscribePatterns);
                    channel.BasicConsume(queue, false, consumer);
                }
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
        public static void RegisterHandlerForFanout<TMessage, THandler>()
            where THandler : BaseHandleMessages<TMessage>
            where TMessage : BaseMessage
        {
            try
            {
                var type = typeof(TMessage);
                var handleType = typeof(THandler);
                if (!dict_func_fanout.ContainsKey(handleType))
                {
                    dict_func_fanout.TryAdd(handleType,true);
                    var handler =  Activator.CreateInstance<THandler>();
                    var channel = connection.CreateModel();
                    var consumer = new EventingBasicConsumer(channel);

                    var exchange = string.Empty;
                    var queue = string.Empty;
                    ConsumerEnsureQueue(channel, type, out exchange, out queue);
                    TMessage msg = null;//闭包获取数据
                    try
                    {
                        consumer.Received += async (model, ea) =>
                        {
                            var json = Encoding.UTF8.GetString(ea.Body);
                            msg = JsonConvert.DeserializeObject<TMessage>(json);
                            await handler.Handle(msg);
                        };
                    }
                    catch (Exception ex)
                    {
                        MessageQueueHelper.UpdateCanbeRemoveIsFalse(msg.MsgHash);
                        throw ex;
                    }

                    channel.BasicConsume(queue, true, consumer);
                }
            }
            catch (Exception ex)
            {
                _logger.Debug("RegisterHandler()出错，异常：" + ex.Message + "；堆栈：" + ex.StackTrace);
            }
        }

        /// <summary>
        /// 发送一条消息，direct模式
        /// </summary>
        /// <typeparam name="TMessage">消息类型</typeparam>
        /// <param name="message">消息</param>
        /// <param name="delaySend">延迟多少毫秒发送消息</param>
        /// <returns>发送成功返回true，否则返回false</returns>
        public static bool Send(BaseMessage message, int delaySend = 0)
        {
            if (string.IsNullOrWhiteSpace(message.Source))
            {
                throw new ArgumentNullException("message.Source");
            }

            if (!PrePersistMessage(message))
            {
                return false;
            }

            var messageType = message.GetType();
            delaySend = delaySend == 0 ? delaySend : Math.Max(delaySend, 1000); // 至少保证1秒的延迟，否则意义不大
            using (var pooled = InnerCreateChannel(messageType))
            {
                IModel channel = pooled.Channel;
                pooled.PreRecord(message.MsgHash);

                var exchange = string.Empty;
                var route_key = string.Empty;
                var queue = string.Empty;
                EnsureQueue(channel, messageType, out exchange, out route_key, out queue, delaySend);

                var json = JsonConvert.SerializeObject(message);
                var bytes = Encoding.UTF8.GetBytes(json);
                var props = channel.CreateBasicProperties();
                props.ContentType = "text/plain";
                props.DeliveryMode = 2;
                channel.BasicPublish(exchange, route_key, props, bytes);
                var time_out = Math.Max(default_retry_wait, message.RetryCount_Publish * 2 /*2倍往上扩大，防止出现均等*/ * 1000);
                var ret = channel.WaitForConfirms(TimeSpan.FromMilliseconds(time_out));
                if (!ret)
                {
                    // 数据库更新该条消息的状态信息
                    if (message.RetryCount_Publish < default_retry_count)
                    {
                        var ok = MessageQueueHelper.Update(
                            message.MsgHash,
                            fromStatus1: MsgStatus.Created, // 之前的状态只能是1 Created 或者2 Retry
                            fromStatus2: MsgStatus.Retrying,
                            toStatus: MsgStatus.Retrying);
                        if (ok)
                        {
                            message.RetryCount_Publish += 1;
                            message.LastRetryTime = DateTime.Now;
                            retry_send_queue.Enqueue(message);
                            return true;
                        }
                        throw new Exception("数据库update出现异常");
                    }
                    throw new Exception($"消息发送超过最大重试次数（{default_retry_count}次）");
                }
            }

            return true;
        }

        /// <summary>
        /// 发布一条消息，topic模式
        /// </summary>
        /// <typeparam name="TMessage"></typeparam>
        /// <param name="message">消息</param>
        /// <param name="pattern">消息满足的模式（也就是routeKey）</param>
        /// <param name="delaySend">延迟多少毫秒发布消息</param>
        /// <returns>发布成功返回true，否则返回false</returns>
        public static bool Publish(BaseMessage message, string pattern, int delaySend = 0)
        {
            if (string.IsNullOrWhiteSpace(message.Source))
            {
                throw new ArgumentNullException("message.Source");
            }

            if (string.IsNullOrWhiteSpace(pattern))
            {
                throw new ArgumentNullException("pattern");
            }

            if (!PrePersistMessage(message))
            {
                return false;
            }

            var messageType = message.GetType();
            delaySend = delaySend == 0 ? delaySend : Math.Max(delaySend, 1000); // 至少保证1秒的延迟，否则意义不大
            using (var pooled = InnerCreateChannel(messageType))
            {
                IModel channel = pooled.Channel;
                pooled.PreRecord(message.MsgHash);

                var exchange = string.Empty;
                var route_key = string.Empty;
                var queue = string.Empty;
                EnsureQueue(channel, messageType, out exchange, out route_key, out queue, pattern, delaySend);

                var json = JsonConvert.SerializeObject(message);
                var bytes = Encoding.UTF8.GetBytes(json);
                var props = channel.CreateBasicProperties();
                props.ContentType = "text/plain";
                props.DeliveryMode = 2;
                channel.BasicPublish(exchange, pattern, props, bytes);
                var time_out = Math.Max(default_retry_wait, message.RetryCount_Publish * 2 /*2倍往上扩大，防止出现均等*/ * 1000);
                var ret = channel.WaitForConfirms(TimeSpan.FromMilliseconds(time_out));
                if (!ret)
                {
                    if (message.RetryCount_Publish < default_retry_count)
                    {
                        var ok = MessageQueueHelper.Update(
                             message.MsgHash,
                             fromStatus1: MsgStatus.Created, // 之前的状态只能是1 Created 或者2 Retry
                             fromStatus2: MsgStatus.Retrying,
                             toStatus: MsgStatus.Retrying);
                        if (ok)
                        {
                            message.RetryCount_Publish += 1;
                            message.LastRetryTime = DateTime.Now;
                            message.Pattern = pattern;
                            retry_pub_queue.Enqueue(message);
                            return true;
                        }
                        throw new Exception("数据库update出现异常");
                    }
                    throw new Exception($"消息发送超过最大重试次数（{default_retry_count}次）");
                }
            }

            return true;
        }

        /// <summary>
        /// fanout模式发布消息,此模式适合两种及以上业务用同一条消息订阅使用，如果只有一种业务或者单机建议用默认模式
        /// </summary>
        /// <param name="message"></param>
        /// <param name="delaySend"></param>
        /// <returns></returns>
        public static bool FanoutPublish(BaseMessage message)
        {
            if (string.IsNullOrWhiteSpace(message.Source))
            {
                throw new ArgumentNullException("message.Source");
            }

            ///fanout模式不同其他模式，又无法获取到订阅者数量，于是直接默认CanbeRemove为true
            ///如果出现异常在设为false，具体在哪儿失败了，怎么恢复就需要调用者自己去查了
            ///数据库消息只提供一个记录，供查证用
            if (!PrePersistMessage(message, true))
            {
                return false;
            }

            var messageType = message.GetType();
            using (var pooled = InnerCreateChannel(messageType))
            {
                IModel channel = pooled.Channel;
               // pooled.PreRecord(message.MsgHash);无需修改状态了

                var exchange = string.Empty;
                PublishEnsureQueue(channel, messageType, out exchange);

                var json = JsonConvert.SerializeObject(message);
                var bytes = Encoding.UTF8.GetBytes(json);
                var props = channel.CreateBasicProperties();
                props.Persistent = true;
                channel.BasicPublish(exchange, "", props, bytes);
                var time_out = Math.Max(default_retry_wait, message.RetryCount_Publish * 2 /*2倍往上扩大，防止出现均等*/ * 1000);
                var ret = channel.WaitForConfirms(TimeSpan.FromMilliseconds(time_out));
                if (!ret)
                {
                    if (message.RetryCount_Publish < default_retry_count)
                    {
                        var ok = MessageQueueHelper.Update(
                             message.MsgHash,
                             fromStatus1: MsgStatus.Created, // 之前的状态只能是1 Created 或者2 Retry
                             fromStatus2: MsgStatus.Retrying,
                             toStatus: MsgStatus.Retrying);
                        if (ok)
                        {
                            message.RetryCount_Publish += 1;
                            message.LastRetryTime = DateTime.Now;
                            retry_fanout_pub_queue.Enqueue(message);
                            return true;
                        }
                        throw new Exception("数据库update出现异常");
                    }
                    throw new Exception($"消息发送超过最大重试次数（{default_retry_count}次）");
                }
            }

            return true;
        }

        private static PooledChannel InnerCreateChannel(Type messageType)
        {
            var pool = pools.GetOrAdd(
                messageType,
                t => new ObjectPool<IPooledWapper>(p => new PooledChannel(connection.CreateModel(), p), 10));
            return pool.Get() as PooledChannel;
        }

        private static bool PrePersistMessage(BaseMessage message, bool isFanout = false)
        {
            if (message.RetryCount_Publish == 0)
            {
                var msgHash = MessageIdHelper.GenerateMessageIdFrom(message.Source);
                if (prepersist.Contains(msgHash))
                {
                    return false;
                }
                else
                {
                    message.MsgHash = msgHash;
                    prepersist.Add(msgHash);
                    if (Interlocked.Increment(ref prepersist_count) >= 1000)
                    {
                        prepersist.RemoveRange(0, 950);
                    }

                    var model = MessageQueueHelper.GetModelBy(msgHash);
                    if (model != null)
                    {
                        return false;
                    }
                    else
                    {
                        var new_model = new MessageQueue
                        {
                            MsgHash = msgHash,
                            MsgContent = message.Source,
                            RetryCount = 0,
                            CanBeRemoved = false,
                            Status = MsgStatus.Created,
                            CreatedTime = DateTime.Now
                        };
                        if (isFanout)
                        {
                            new_model.CanBeRemoved = true;
                        }
                        MessageQueueHelper.Insert(new_model);
                        return true;
                    }
                }
            }
            else // RetryCount > 0
            {
                // 直接返回true，以便后续可以进行重发
                return true;
            }
        }

        private static void EnsureQueue(IModel channel, Type messageType, out string exchange, out string routeKey, out string queue, int delaySend = 0)
        {
            var type = messageType;
            if (!dict_info.ContainsKey(type))
            {
                var info = GetQueueInfo(type);
                exchange = info.Exchange;
                routeKey = info.DefaultRouteKey;
                queue = info.Queue;

                channel.ExchangeDeclare(exchange, ExchangeType.Direct, durable: true);
                channel.QueueDeclare(queue, durable: true, exclusive: false, autoDelete: false);
                channel.QueueBind(queue, exchange, routeKey);

                if (delaySend > 0)
                {
                    var args = new Dictionary<string, object>();
                    args.Add("x-message-ttl", delaySend);
                    args.Add("x-dead-letter-exchange", exchange);
                    args.Add("x-dead-letter-routing-key", queue);
                    channel.QueueDeclare(queue + ".delay", durable: false, exclusive: false, autoDelete: false, arguments: args);
                    exchange = string.Empty;
                    routeKey = info.Queue + ".delay";
                    queue = info.Queue + ".delay";
                }
            }
            else
            {
                var info = GetQueueInfo(type);
                exchange = info.Exchange;
                routeKey = info.DefaultRouteKey;
                queue = info.Queue;
                if (delaySend > 0)
                {
                    exchange = string.Empty;
                    routeKey = info.Queue + ".delay";
                    queue = info.Queue + ".delay";
                }
            }
        }

        private static void EnsureQueue(IModel channel, Type messageType, string subscriberName, out string exchange, out string queue, params string[] subscribePatterns)
        {
            var key = (messageType, subscriberName);
            if (!dict_info_name.ContainsKey(key))
            {
                var info = GetQueueInfo(messageType, subscriberName);
                exchange = "topic." + info.Exchange;
                queue = info.Queue + "." + subscriberName;
                channel.ExchangeDeclare(exchange, ExchangeType.Topic, durable: true);
                channel.QueueDeclare(queue, durable: true, exclusive: false, autoDelete: false);
                foreach (var pattern in subscribePatterns)
                {
                    channel.QueueBind(queue, exchange, routingKey: pattern);
                }
            }
            else
            {
                var info = GetQueueInfo(messageType, subscriberName);
                exchange = "topic." + info.Exchange;
                queue = info.Queue + "." + subscriberName;
            }
        }

        private static void EnsureQueue(IModel channel, Type messageType, out string exchange, out string routeKey, out string queue, string pattern, int delaySend = 0)
        {
            var type = messageType;
            if (!dict_info.ContainsKey(type))
            {
                var info = GetQueueInfo(type);
                exchange = "topic." + info.Exchange;
                routeKey = pattern;
                queue = info.Queue;
                channel.ExchangeDeclare(exchange, ExchangeType.Topic, durable: true);

                if (delaySend > 0)
                {
                    var args = new Dictionary<string, object>();
                    args.Add("x-message-ttl", delaySend);
                    args.Add("x-dead-letter-exchange", exchange);
                    args.Add("x-dead-letter-routing-key", pattern);
                    channel.QueueDeclare(queue + ".delay", durable: true, exclusive: false, autoDelete: false, arguments: args);
                    exchange = string.Empty;
                    routeKey = info.Queue + ".delay";
                    queue = info.Queue + ".delay";
                }
            }
            else
            {
                var info = GetQueueInfo(type);
                exchange = "topic." + info.Exchange;
                routeKey = pattern;
                queue = info.Queue;
                if (delaySend > 0)
                {
                    exchange = string.Empty;
                    routeKey = info.Queue + ".delay";
                    queue = info.Queue + ".delay";
                }
            }
        }

        private static void PublishEnsureQueue(IModel channel, Type messageType, out string exchange)
        {
            var type = messageType;
            if (!dict_info_fanout.ContainsKey(type))
            {
                var info = GetQueueInfoForFanout(messageType);
                exchange = info;
                channel.ExchangeDeclare(exchange, ExchangeType.Fanout, durable: true);
            }
            else
            {
                var info = GetQueueInfoForFanout(messageType);
                exchange = info;
            }
        }

        private static void ConsumerEnsureQueue(IModel channel, Type messageType, out string exchange, out string queue)
        {
            var info = GetQueueInfoForFanout(messageType);
            exchange = info;
            channel.ExchangeDeclare(exchange, ExchangeType.Fanout, durable: true);
            queue = channel.QueueDeclare().QueueName;
            channel.QueueBind(queue: queue, exchange: exchange, routingKey: "", arguments: null);

        }

        private static QueueInfo GetQueueInfo(Type messageType)
        {
            var type_name = messageType.IsGenericType ? messageType.GenericTypeArguments[0].Name : messageType.Name;
            var info = dict_info.GetOrAdd(messageType, t => new QueueInfo
            {
                Exchange = type_name + ".exchange",
                DefaultRouteKey = type_name + ".input",
                Queue = type_name + ".input"
            });

            return info;
        }

        private static QueueInfo GetQueueInfo(Type messageType, string name)
        {
            var type_name = messageType.IsGenericType ? messageType.GenericTypeArguments[0].Name + "[" + name + "]" : messageType.Name;
            var info = dict_info.GetOrAdd(messageType, t => new QueueInfo
            {
                Exchange = type_name + ".exchange",
                DefaultRouteKey = type_name + ".input",
                Queue = type_name + ".input"
            });

            return info;
        }

        private static string GetQueueInfoForFanout(Type messageType)
        {
            var type_name = messageType.IsGenericType ? messageType.GenericTypeArguments[0].Name : messageType.Name;
            var info = dict_info_fanout.GetOrAdd(messageType, type_name + ".exchange");

            return info;
        }

        private class QueueInfo
        {
            public string Exchange;
            public string DefaultRouteKey;
            public string Queue;
        }
    }
}
