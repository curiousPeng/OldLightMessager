using Dapper;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace LightMessager.DAL
{
    internal partial class MessageQueueHelper : BaseTableHelper
    {
        /// <summary>
        /// 是否存在指定的MessageQueue实体对象
        /// </summary>
        /// <param name="Id">Id</param>
        /// <returns>是否存在，true为存在</returns>
        public static bool Exists(int Id)
        {
            var sql = new StringBuilder();
            sql.Append("SELECT COUNT(1) FROM [MessageQueue]");
            sql.Append(" WHERE [Id]=@Id ");
            var ret = false;
            using (var conn = GetOpenConnection())
            {
                ret = conn.ExecuteScalar<int>(sql.ToString(), new { @Id = Id }) > 0;
            }

            return ret;
        }

        /// <summary>
        /// 添加MessageQueue实体对象
        /// </summary>
        /// <param name="model">MessageQueue实体</param>
        /// <returns>新插入数据的id</returns>
        public static int Insert(MessageQueue model)
        {
            var sql = new StringBuilder();
            sql.Append("INSERT INTO [MessageQueue]([MsgHash], [MsgContent], [Status], [RetryCount], [LastRetryTime], [CanBeRemoved], [CreatedTime])");
            sql.Append(" OUTPUT INSERTED.[Id] ");
            sql.Append("VALUES(@MsgHash, @MsgContent, @Status, @RetryCount, @LastRetryTime, @CanBeRemoved, @CreatedTime)");
            var ret = 0;
            using (var conn = GetOpenConnection())
            {
                ret = conn.ExecuteScalar<int>(sql.ToString(), model);
            }

            return ret;
        }

        /// <summary>
        /// 删除指定的MessageQueue实体对象
        /// </summary>
        /// <param name="Id">Id</param>
        /// <returns>是否成功，true为成功</returns>
        public static bool Delete(int Id)
        {
            var sql = new StringBuilder();
            sql.Append("DELETE FROM [MessageQueue] ");
            sql.Append(" WHERE [Id]=@Id ");
            var ret = false;
            using (var conn = GetOpenConnection())
            {
                ret = conn.Execute(sql.ToString(), new { @Id = Id }) > 0;
            }

            return ret;
        }

        /// <summary>
        /// 批量删除指定的MessageQueue实体对象
        /// </summary>
        /// <param name="ids">id列表 id列表</param>
        /// <returns>是否成功，true为成功</returns>
        public static bool Delete(List<int> ids)
        {
            var sql = new StringBuilder();
            sql.Append("DELETE FROM [MessageQueue] ");
            sql.Append(" WHERE [Id] IN @ids");
            var ret = false;
            using (var conn = GetOpenConnection())
            {
                ret = conn.Execute(sql.ToString(), new { @ids = ids }) > 0;
            }

            return ret;
        }

        public static bool Update(long msgHash, short fromStatus, short toStatus)
        {
            var sql = new StringBuilder();
            sql.AppendLine("DECLARE @retVal int ");
            sql.AppendLine("UPDATE [MessageQueue] ");
            sql.AppendLine("SET [Status]=@toStatus, [RetryCount]=[RetryCount]+1, [LastRetryTime]=@LastRetryTime, [CanBeRemoved]=@CanBeRemoved ");
            sql.AppendLine("WHERE [MsgHash]=@MsgHash and [Status]=@fromStatus");
            sql.AppendLine("SELECT @retVal = @@Rowcount ");
            sql.AppendLine("IF (@retVal = 0) BEGIN");
            sql.AppendLine("UPDATE [MessageQueue] set [Status]=5 WHERE [MsgHash]=@MsgHash END SELECT @retVal"); // 5 Exception
            var ret = false;
            using (var conn = GetOpenConnection())
            {
                ret = conn.ExecuteScalar<int>(sql.ToString(), new
                {
                    @MsgHash = msgHash,
                    @fromStatus = fromStatus,
                    @toStatus = toStatus,
                    @LastRetryTime = DateTime.Now,
                    @CanBeRemoved = toStatus == 6 ? true : false // 6 Processed
                }) > 0;
            }

            return ret;
        }

        public static bool UpdateCanbeRemoveIsFalse(long msgHash)
        {
            var sql = new StringBuilder();
            sql.AppendLine("UPDATE [MessageQueue] ");
            sql.AppendLine("SET [CanBeRemoved]=@CanBeRemoved ");
            sql.AppendLine("WHERE [MsgHash]=@MsgHash");
           
            var ret = false;
            using (var conn = GetOpenConnection())
            {
                ret = conn.ExecuteScalar<int>(sql.ToString(), new
                {
                    @MsgHash = msgHash,
                    @CanBeRemoved = true
                }) > 0;
            }

            return ret;
        }

        public static bool Update(long msgHash, short fromStatus1, short fromStatus2, short toStatus)
        {
            var sql = new StringBuilder();
            sql.AppendLine("DECLARE @retVal int ");
            sql.AppendLine("UPDATE [MessageQueue] ");
            sql.AppendLine("SET [Status]=@toStatus, [RetryCount]=[RetryCount]+1, [LastRetryTime]=@LastRetryTime, [CanBeRemoved]=@CanBeRemoved ");
            sql.AppendLine("WHERE [MsgHash]=@MsgHash and ([Status]=@fromStatus1 or [Status]=@fromStatus1)");
            sql.AppendLine("SELECT @retVal = @@Rowcount ");
            sql.AppendLine("IF (@retVal = 0) BEGIN");
            sql.AppendLine("UPDATE [MessageQueue] set [Status]=5 WHERE [MsgHash]=@MsgHash END SELECT @retVal"); // 5 Exception
            var ret = false;
            using (var conn = GetOpenConnection())
            {
                ret = conn.ExecuteScalar<int>(sql.ToString(), new
                {
                    @MsgHash = msgHash,
                    @fromStatus1 = fromStatus1,
                    @fromStatus2 = fromStatus2,
                    @toStatus = toStatus,
                    @LastRetryTime = DateTime.Now,
                    @CanBeRemoved = toStatus == 6 ? true : false // 6 Processed
                }) > 0;
            }

            return ret;
        }

        /// <summary>
        /// 获取指定的MessageQueue实体对象
        /// </summary>
        /// <param name="Id">Id</param>
        /// <returns>MessageQueue实体对象</returns>
        public static MessageQueue GetModel(int Id)
        {
            var sql = new StringBuilder();
            sql.Append("SELECT TOP 1 [Id], [MsgHash], [MsgContent], [Status], [RetryCount], [LastRetryTime], [CanBeRemoved], [CreatedTime] FROM [MessageQueue] ");
            sql.Append(" WHERE [Id]=@Id ");
            MessageQueue ret = null;
            using (var conn = GetOpenConnection())
            {
                ret = conn.QueryFirstOrDefault<MessageQueue>(sql.ToString(), new { @Id = Id });
            }

            return ret;
        }

        /// <summary>
        /// 获取指定的MessageQueue实体对象
        /// </summary>
        /// <param name="Id">Id</param>
        /// <returns>MessageQueue实体对象</returns>
        public static MessageQueue GetModelBy(long msgHash)
        {
            var sql = new StringBuilder();
            sql.Append("SELECT TOP 1 [Id], [MsgHash], [MsgContent], [CanBeRemoved], [RetryCount], [LastRetryTime], [CreatedTime] FROM [MessageQueue] ");
            sql.Append(" WHERE [MsgHash]=@MsgHash");
            MessageQueue ret = null;
            using (var conn = GetOpenConnection())
            {
                ret = conn.QueryFirstOrDefault<MessageQueue>(sql.ToString(), new { @MsgHash = msgHash });
            }

            return ret;
        }

        /// <summary>
        /// 批量获取MessageQueue实体对象
        /// </summary>
        /// <param name="where">查询条件（不需要带有where关键字）</param>
        /// <param name="top">取出前top数的数据</param>
        /// <returns>MessageQueue实体对象列表</returns>
        public static List<MessageQueue> GetList(string where = "", int top = 100)
        {
            var sql = new StringBuilder();
            sql.Append("SELECT ");
            sql.Append(" TOP " + top.ToString());
            sql.Append(" [Id], [MsgHash], [MsgContent], [Status], [RetryCount], [LastRetryTime], [CanBeRemoved], [CreatedTime] ");
            sql.Append(" FROM [MessageQueue] ");
            if (!string.IsNullOrWhiteSpace(where))
            {
                if (where.ToLower().Contains("where"))
                {
                    throw new ArgumentException("where子句不需要带where关键字");
                }
                sql.Append(" WHERE " + where);
            }
            object ret = null;
            using (var conn = GetOpenConnection())
            {
                ret = conn.Query<MessageQueue>(sql.ToString()).ToList();
            }

            return (List<MessageQueue>)ret;
        }

        /// <summary>
        /// 获取记录总数
        /// </summary>
        /// <param name="where">查询条件（不需要带有where关键字）</param>
        public static int GetCount(string where = "")
        {
            var sql = new StringBuilder();
            sql.Append("SELECT COUNT(1) FROM [MessageQueue] ");
            if (!string.IsNullOrWhiteSpace(where))
            {
                if (where.ToLower().Contains("where"))
                {
                    throw new ArgumentException("where子句不需要带where关键字");
                }
                sql.Append(" WHERE " + where);
            }
            var ret = -1;
            using (var conn = GetOpenConnection())
            {
                ret = conn.ExecuteScalar<int>(sql.ToString());
            }

            return ret;
        }

        /// <summary>
        /// 分页获取数据列表
        /// </summary>
        public static PageDataView<MessageQueue> GetListByPage(string where = "", string orderBy = "", string columns = " * ", int pageSize = 20, int currentPage = 1)
        {
            return Paged<MessageQueue>("MessageQueue", where, orderBy, columns, pageSize, currentPage);
        }
    }
}
