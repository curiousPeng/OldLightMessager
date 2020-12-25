using LightMessager.DAL;
using LightMessager.DAL.Model;
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
    public sealed class RabbitMQProducer : IRabbitMQProducer
    {
        static ConnectionFactory factory;
        static IConnection connection;
        static volatile int prepersist_count;
        static readonly int default_retry_wait;
        static readonly int default_retry_count;
        static List<long> prepersist;
        static ConcurrentQueue<BaseMessage> direct_queue;
        static ConcurrentQueue<BaseMessage> topic_queue;
        static ConcurrentQueue<BaseMessage> fanout_queue;
        static ConcurrentDictionary<Type, QueueInfo> dict_info;
        static ConcurrentDictionary<string, QueueInfo> dict_info_custom;
        static ConcurrentDictionary<Type, ObjectPool<IPooledWapper>> pools;
        private static IMessageQueueHelper _message_queue_helper;

        public RabbitMQProducer(IConfiguration configurationRoot, IMessageQueueHelper messageQueueHelper)
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
            _message_queue_helper = messageQueueHelper;
        }
        static RabbitMQProducer()
        {
            prepersist_count = 0;
            default_retry_wait = 1000; // 1秒
            default_retry_count = 3; // 消息重试最大3次
            prepersist = new List<long>();
            direct_queue = new ConcurrentQueue<BaseMessage>();
            topic_queue = new ConcurrentQueue<BaseMessage>();
            fanout_queue = new ConcurrentQueue<BaseMessage>();
            dict_info = new ConcurrentDictionary<Type, QueueInfo>();
            dict_info_custom = new ConcurrentDictionary<string, QueueInfo>();
            pools = new ConcurrentDictionary<Type, ObjectPool<IPooledWapper>>();

            //开启轮询检测，扫描重试队列，重发消息
            new Thread(() =>
            {
                // 先实现为spin的方式，后面考虑换成blockingqueue的方式
                while (true)
                {
                    BaseMessage send_item;
                    while (direct_queue.TryDequeue(out send_item))
                    {
                        SendDirect(send_item);
                    }

                    BaseMessage pub_item;
                    while (topic_queue.TryDequeue(out pub_item))
                    {
                        SendTopic(pub_item, pub_item.routeKey);
                    }

                    BaseMessage pub_item_fanout;
                    while (fanout_queue.TryDequeue(out pub_item_fanout))
                    {
                        SendFanout(pub_item_fanout);
                    }
                    Thread.Sleep(1000 * 5);
                }
            }).Start();
        }

        /// <summary>
        /// 发送一条消息，默认的direct模式,自定义exchangeName,queueName
        /// </summary>
        /// <param name="message">消息</param>
        /// <param name="exchangeName">exchangeName</param>
        /// <param name="queueName">队列名</param>
        /// <param name="routeKey">路由键，不定义就默认使用队列名做路由键</param>
        /// <param name="delaySend">延迟多少毫秒发送消息,一般不低于5000</param>
        /// <returns>发送成功返回true，否则返回false</returns>
        public bool DirectSend(BaseMessage message, string exchangeName = "", string queueName = "", string routeKey = "", int delaySend = 0)
        {
            return SendDirect(message, exchangeName, queueName, routeKey, delaySend);
        }

        /// <summary>
        /// topic模式发送消息
        /// </summary>
        /// <param name="message">消息</param>
        /// <param name="exchangeName">exchangeName</param>
        /// <param name="queueName">队列名</param>
        /// <param name="routeKey">路由键</param>
        /// <param name="delaySend">延迟多少毫秒发布消息</param>
        /// <returns>发布成功返回true，否则返回false</returns>
        public bool TopicSend(BaseMessage message, string routeKey, string exchangeName = "", string queueName = "", int delaySend = 0)
        {
            return SendTopic(message, routeKey, exchangeName, queueName, delaySend);
        }

        /// <summary>
        /// fanout模式发布消息
        /// 此模式适合两种及以上业务用同一条消息订阅使用，如果只有一种业务或者单机建议用默认模式
        /// </summary>
        /// <param name="message"></param>
        /// <param name="exchangeName"></param>
        /// <param name="queueName"></param>
        /// <param name="delaySend"></param>
        /// <returns></returns>
        public bool FanoutSend(BaseMessage message, string exchangeName = "", string queueName = "", int delaySend = 0)
        {
            return SendFanout(message, exchangeName, queueName, delaySend);
        }

        private static bool SendDirect(BaseMessage message, string exchangeName = "", string queueName = "", string routeKey = "", int delaySend = 0)
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
            delaySend = delaySend == 0 ? delaySend : Math.Max(delaySend, 5000); // 至少保证5秒的延迟，否则意义不大
            using (var pooled = InnerCreateChannel(messageType))
            {
                IModel channel = pooled.Channel;
                pooled.PreRecord(message.MsgHash);

                var exchange = string.Empty;
                var queue = string.Empty;
                if (!string.IsNullOrEmpty(exchange) && !string.IsNullOrEmpty(queue) && !string.IsNullOrEmpty(routeKey))
                {
                    exchange = exchangeName;
                    queue = queueName;
                    DirectEnsureQueue(channel, ref exchange, ref queue, routeKey, delaySend);
                }
                else
                {
                    DirectEnsureQueue(channel, messageType, out exchange, out queue, delaySend);
                }

                var route_key = queue;
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
                        var ok = _message_queue_helper.Update(
                            message.MsgHash,
                            fromStatus1: MsgStatus.Created, // 之前的状态只能是1 Created 或者2 Retry
                            fromStatus2: MsgStatus.Retrying,
                            toStatus: MsgStatus.Retrying);
                        if (ok)
                        {
                            message.RetryCount_Publish += 1;
                            message.LastRetryTime = DateTime.Now;
                            direct_queue.Enqueue(message);
                            return true;
                        }
                        throw new Exception("数据库update出现异常");
                    }
                    throw new Exception($"消息发送超过最大重试次数（{default_retry_count}次）");
                }
            }

            return true;
        }
        private static bool SendTopic(BaseMessage message, string routeKey, string exchangeName = "", string queueName = "", int delaySend = 0)
        {
            if (string.IsNullOrWhiteSpace(message.Source))
            {
                throw new ArgumentNullException("message.Source");
            }

            if (string.IsNullOrWhiteSpace(routeKey))
            {
                throw new ArgumentNullException("routeKey");
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
                var queue = string.Empty;
                if (!string.IsNullOrEmpty(exchangeName) && !string.IsNullOrEmpty(queueName))
                {
                    exchange = exchangeName;
                    queue = queueName;
                    TopicEnsureQueue(channel, routeKey, ref exchange, ref queue, delaySend);
                }
                else
                {
                    TopicEnsureQueue(channel, messageType, routeKey, out exchange, out queue, delaySend);
                }

                var json = JsonConvert.SerializeObject(message);
                var bytes = Encoding.UTF8.GetBytes(json);
                var props = channel.CreateBasicProperties();
                props.ContentType = "text/plain";
                props.DeliveryMode = 2;
                channel.BasicPublish(exchange, routeKey, props, bytes);
                var time_out = Math.Max(default_retry_wait, message.RetryCount_Publish * 2 /*2倍往上扩大，防止出现均等*/ * 1000);
                var ret = channel.WaitForConfirms(TimeSpan.FromMilliseconds(time_out));
                if (!ret)
                {
                    if (message.RetryCount_Publish < default_retry_count)
                    {
                        var ok = _message_queue_helper.Update(
                             message.MsgHash,
                             fromStatus1: MsgStatus.Created, // 之前的状态只能是1 Created 或者2 Retry
                             fromStatus2: MsgStatus.Retrying,
                             toStatus: MsgStatus.Retrying);
                        if (ok)
                        {
                            message.RetryCount_Publish += 1;
                            message.LastRetryTime = DateTime.Now;
                            message.routeKey = routeKey;
                            topic_queue.Enqueue(message);
                            return true;
                        }
                        throw new Exception("数据库update出现异常");
                    }
                    throw new Exception($"消息发送超过最大重试次数（{default_retry_count}次）");
                }
            }

            return true;
        }
        private static bool SendFanout(BaseMessage message, string exchangeName = "", string queueName = "", int delaySend = 0)
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
                if (string.IsNullOrEmpty(exchangeName) ||  string.IsNullOrEmpty(queueName))
                {
                    FanoutEnsureQueue(channel, messageType, out exchange, delaySend);
                }
                else
                {
                    exchange = exchangeName;
                    FanoutEnsureQueue(channel, ref exchange, ref queueName, delaySend);
                }


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
                        var ok = _message_queue_helper.Update(
                             message.MsgHash,
                             fromStatus1: MsgStatus.Created, // 之前的状态只能是1 Created 或者2 Retry
                             fromStatus2: MsgStatus.Retrying,
                             toStatus: MsgStatus.Retrying);
                        if (ok)
                        {
                            message.RetryCount_Publish += 1;
                            message.LastRetryTime = DateTime.Now;
                            fanout_queue.Enqueue(message);
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
                t => new ObjectPool<IPooledWapper>(p => new PooledChannel(connection.CreateModel(), p, _message_queue_helper), 10));
            return pool.Get() as PooledChannel;
        }

        private static bool PrePersistMessage(BaseMessage message, bool isFanout = false)
        {
            if (message.RetryCount_Publish == 0)
            {
                var msgHash = GenerateMessageIdFrom(message.Source);
                if (prepersist.Contains(msgHash))
                {
                    return false;
                }
                else
                {
                    message.MsgHash = msgHash;
                    if (Interlocked.Increment(ref prepersist_count) == 1000)
                    {
                        prepersist.RemoveRange(0, 950);
                    }
                    prepersist.Add(msgHash);

                    var model = _message_queue_helper.GetModelBy(msgHash);
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
                        _message_queue_helper.Insert(new_model);
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

        private static void DirectEnsureQueue(IModel channel, Type messageType, out string exchange, out string queue, int delaySend = 0)
        {
            var type = messageType;
            if (delaySend > 0)
            {//走新加延迟队列
                var type_name = messageType.IsGenericType ? messageType.GenericTypeArguments[0].Name : messageType.Name;
                var key = type_name + ".exchange.delay";
                exchange = key;
                queue = type_name + ".input.delay";
                if (!dict_info_custom.ContainsKey(key))
                {
                    var routeKey = type_name + ".input";
                    GetQueueInfoForCustom(key, exchange, queue, routeKey);
                    channel.ExchangeDeclare(exchange, ExchangeType.Direct, durable: true);
                    var args = new Dictionary<string, object>();
                    args.Add("x-message-ttl", delaySend);
                    args.Add("x-dead-letter-exchange", type_name + ".exchange");//死掉过后转发到的exchange
                    args.Add("x-dead-letter-routing-key", routeKey);//转发的exchange的路由键
                    channel.QueueDeclare(queue, durable: false, exclusive: false, autoDelete: false, arguments: args);
                    channel.QueueBind(queue, exchange, routeKey, null);
                }
            }
            else
            {
                var info = GetQueueInfo(type);
                exchange = info.Exchange;
                queue = info.Queue;
                if (!dict_info.ContainsKey(type))
                {
                    channel.ExchangeDeclare(exchange, ExchangeType.Direct, durable: true);
                    channel.QueueDeclare(queue, durable: true, exclusive: false, autoDelete: false);
                    channel.QueueBind(queue, exchange, queue);
                }
            }
        }
        private static void DirectEnsureQueue(IModel channel, ref string exchange, ref string queue, string routeKey = null, int delaySend = 0)
        {
            string key;
            string realExchange = exchange;
            string realQueue = queue;
            if (delaySend > 0)
            {
                exchange = exchange + ".delay";
                queue = queue + ".delay";
            }
            key = queue;///为什么这儿是queue，而上面是exchange，因为上面是工具定义的，
            ///只要是同一个消息结构，queueName是定死了，所以直接返回Exchange就行了
            ///而下面是用户自定义的queue，同一个exchange下可以有多个不同的queue，绑定就不同
            if (!dict_info_custom.ContainsKey(key))
            {
                GetQueueInfoForCustom(key, exchange, queue, routeKey);
                channel.ExchangeDeclare(exchange, ExchangeType.Direct, durable: true);
                if (delaySend > 0)
                {
                    var args = new Dictionary<string, object>();
                    args.Add("x-message-ttl", delaySend);
                    args.Add("x-dead-letter-exchange", realExchange);
                    args.Add("x-dead-letter-routing-key", string.IsNullOrEmpty(routeKey) ? realQueue : routeKey);
                    channel.QueueDeclare(queue, durable: false, exclusive: false, autoDelete: false, arguments: args);
                }
                else
                {
                    channel.QueueDeclare(queue, durable: true, exclusive: false, autoDelete: false);
                }
                channel.QueueBind(queue, exchange, string.IsNullOrEmpty(routeKey) ? queue : routeKey);
            }
        }

        private static void TopicEnsureQueue(IModel channel, Type messageType, string routeKey, out string exchange, out string queue, int delaySend = 0)
        {
            var type = messageType;
            if (delaySend > 0)
            {//走新加延迟队列
                var type_name = messageType.IsGenericType ? messageType.GenericTypeArguments[0].Name : messageType.Name;
                var key = routeKey;///为什么direct模式用Queue，而这里这里又用routekey
                                   ///因为topic模式唯一指定的就是routekey，其他的都不定
                exchange = string.Format("{0}{1}{2}", "topic.", type_name, ".exchange.delay");
                queue = type_name + ".input.delay";
                if (!dict_info_custom.ContainsKey(key))
                {
                    GetQueueInfoForCustom(key, exchange, queue, routeKey);
                    channel.ExchangeDeclare(exchange, ExchangeType.Topic, durable: true);
                    var args = new Dictionary<string, object>();
                    args.Add("x-message-ttl", delaySend);
                    args.Add("x-dead-letter-exchange", string.Format("{0}{1}{2}", "topic.", type_name, ".exchange"));//死掉过后转发到的exchange
                    args.Add("x-dead-letter-routing-key", routeKey);//转发的exchange的路由键
                    channel.QueueDeclare(queue, durable: false, exclusive: false, autoDelete: false, arguments: args);
                    channel.QueueBind(queue, exchange, routeKey, null);
                }
            }
            else
            {
                var info = GetQueueInfo(type);
                exchange = "topic." + info.Exchange;
                queue = info.Queue;
                if (!dict_info.ContainsKey(type))
                {
                    channel.ExchangeDeclare(exchange, ExchangeType.Topic, durable: true);
                    channel.QueueDeclare(queue, durable: true, exclusive: false, autoDelete: false);
                    channel.QueueBind(queue, exchange, routeKey);
                }
            }
        }
        private static void TopicEnsureQueue(IModel channel, string routeKey, ref string exchange, ref string queue, int delaySend = 0)
        {
            string realExchange = exchange;
            if (delaySend > 0)
            {
                exchange = exchange + ".delay";
                queue = queue + ".delay";
            }
            var key = routeKey;
            if (!dict_info_custom.ContainsKey(key))
            {
                GetQueueInfoForCustom(key, exchange, queue, routeKey);
                channel.ExchangeDeclare(exchange, ExchangeType.Topic, durable: true);

                if (delaySend > 0)
                {
                    var args = new Dictionary<string, object>();
                    args.Add("x-message-ttl", delaySend);
                    args.Add("x-dead-letter-exchange", realExchange);
                    args.Add("x-dead-letter-routing-key", routeKey);
                    channel.QueueDeclare(queue, durable: true, exclusive: false, autoDelete: false, arguments: args);
                }
                else
                {
                    channel.QueueDeclare(queue, durable: true, exclusive: false, autoDelete: false);
                }
                channel.QueueBind(queue, exchange, routeKey);
            }
        }

        private static void FanoutEnsureQueue(IModel channel, Type messageType, out string exchange, int delaySend = 0)
        {
            var type = messageType;
            if (delaySend > 0)
            {
                var type_name = messageType.IsGenericType ? messageType.GenericTypeArguments[0].Name : messageType.Name;
                var key = string.Format("{0}{1}{2}", "fanout.", type_name, ".exchange.delay");
                exchange = key;///同direct模式
                var queue = type_name + ".input.delay";
                if (!dict_info_custom.ContainsKey(key))
                {
                    GetQueueInfoForCustom(key, exchange, queue, "");
                    channel.ExchangeDeclare(exchange, ExchangeType.Fanout, durable: true);
                    var args = new Dictionary<string, object>();
                    args.Add("x-message-ttl", delaySend);
                    args.Add("x-dead-letter-exchange", string.Format("{0}{1}{2}", "fanout.", type_name, ".exchange"));//死掉过后转发到的exchange
                    args.Add("x-dead-letter-routing-key", "");//转发的exchange的路由键
                    channel.QueueDeclare(queue, durable: false, exclusive: false, autoDelete: false, arguments: args);
                    channel.QueueBind(queue, exchange, "", null);
                }
            }
            else
            {
                var info = GetQueueInfo(messageType);
                exchange = info.Exchange;
                if (!dict_info.ContainsKey(type))
                {
                    channel.ExchangeDeclare(exchange, ExchangeType.Fanout, durable: true);
                }
            }
        }

        private static void FanoutEnsureQueue(IModel channel, ref string exchange, ref string queue, int delaySend = 0)
        {
            string realExchange = exchange;
            if (delaySend > 0)
            {
                exchange = exchange + ".delay";
                queue = queue + ".delay";
            }
            var key = queue;
            if (!dict_info_custom.ContainsKey(key))
            {
                GetQueueInfoForCustom(key, exchange, queue, "");
                channel.ExchangeDeclare(exchange, ExchangeType.Fanout, durable: true);

                if (delaySend > 0)
                {
                    var args = new Dictionary<string, object>();
                    args.Add("x-message-ttl", delaySend);
                    args.Add("x-dead-letter-exchange", realExchange);
                    args.Add("x-dead-letter-routing-key", "");
                    channel.QueueDeclare(queue, durable: true, exclusive: false, autoDelete: false, arguments: args);
                }
                else
                {
                    channel.QueueDeclare(queue, durable: true, exclusive: false, autoDelete: false);
                }
                channel.QueueBind(queue, exchange, "");
            }
        }

        private static QueueInfo GetQueueInfo(Type messageType)
        {
            var type_name = messageType.IsGenericType ? messageType.GenericTypeArguments[0].Name : messageType.Name;
            var info = dict_info.GetOrAdd(messageType, t => new QueueInfo
            {
                Exchange = type_name + ".exchange",
                RouteKey = type_name + ".input",
                Queue = type_name + ".input"
            });

            return info;
        }

        private static QueueInfo GetQueueInfoForCustom(string key, string exchangeName, string queueName, string routeName)
        {
            var info = dict_info_custom.GetOrAdd(key, t => new QueueInfo
            {
                Exchange = exchangeName,
                RouteKey = routeName,
                Queue = queueName
            });

            return info;
        }
        public static long GenerateMessageIdFrom(string str)
        {
            return str.GetHashCode();
        }
    }
}
