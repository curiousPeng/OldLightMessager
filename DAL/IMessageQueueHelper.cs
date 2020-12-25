using LightMessager.DAL.Model;
using System;
using System.Collections.Generic;
using System.Text;

namespace LightMessager.DAL
{
    public interface IMessageQueueHelper
    {
        int Insert(MessageQueue model);
        bool Update(long msgHash, short fromStatus, short toStatus);
        bool UpdateCanbeRemoveIsFalse(long msgHash);
        bool Update(long msgHash, short fromStatus1, short fromStatus2, short toStatus);
        MessageQueue GetModelBy(long msgHash);
    }
}
