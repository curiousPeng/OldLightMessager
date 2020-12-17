﻿using LightMessager.DAL;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;

namespace LightMessager.Pool
{
    internal class PooledChannel : IPooledWapper
    {
        private IModel _internalChannel;
        private ObjectPool<IPooledWapper> _pool;
        private Dictionary<ulong, long> _unconfirm;
        public DateTime LastGetTime { set; get; }
        public IModel Channel { get { return this._internalChannel; } }

        public PooledChannel(IModel channel, ObjectPool<IPooledWapper> pool)
        {
            _pool = pool;
            _unconfirm = new Dictionary<ulong, long>();
            _internalChannel = channel;
            _internalChannel.ConfirmSelect();
            // 此处不考虑BasicReturn的情况，因为消息发送并没有指定mandatory属性
            _internalChannel.BasicAcks += Channel_BasicAcks;
            _internalChannel.BasicNacks += Channel_BasicNacks;
            _internalChannel.ModelShutdown += Channel_ModelShutdown;
        }

        internal void PreRecord(long msgHash)
        {
            _unconfirm.Add(_internalChannel.NextPublishSeqNo, msgHash);
        }

        private void Channel_BasicNacks(object sender, BasicNackEventArgs e)
        {
            // 可以不做操作，消息的状态维持在初始的created也是可行的
        }

        // broker正常接受到消息，会触发该ack事件
        private void Channel_BasicAcks(object sender, BasicAckEventArgs e)
        {
            // 数据更新该条消息的状态信息
            long msgHash = 0;
            if (_unconfirm.TryGetValue(e.DeliveryTag, out msgHash))
            {
                var ok = MessageQueueHelper.Update(
                    msgHash,
                    fromStatus1: MsgStatus.Created, // 之前的状态只能是1 Created 或者2 Retry
                    fromStatus2: MsgStatus.Retrying,
                    toStatus: MsgStatus.ArrivedBroker);
                if (ok)
                {
                    _unconfirm.Remove(e.DeliveryTag);
                }
                else
                {
                    throw new Exception("数据库update出现异常");
                }
            }
        }

        private void Channel_ModelShutdown(object sender, ShutdownEventArgs e)
        {
            _unconfirm.Clear();
            _internalChannel.BasicAcks -= Channel_BasicAcks;
            _internalChannel.BasicNacks -= Channel_BasicNacks;
            _internalChannel.ModelShutdown -= Channel_ModelShutdown;
        }

        public void Dispose()
        {
            // 必须为true
            Dispose(true);
            // 通知垃圾回收机制不再调用终结器（析构器）
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                // 清理托管资源
                if (_pool.IsDisposed)
                {
                    _internalChannel.Dispose();
                }
                else
                {
                    _unconfirm.Clear();
                    _pool.Put(this);
                }
            }

            // 清理非托管资源
        }
    }
}
