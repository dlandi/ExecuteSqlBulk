using System.Collections.Generic;
using Microsoft.Data.SqlClient;

namespace ExecuteSqlBulk
{
    internal class SqlBulkInsert : SqlBulkBase
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="tran"></param>
        internal SqlBulkInsert(SqlConnection connection, SqlTransaction tran)
        {
            SqlBulk(connection, tran, SqlBulkCopyOptions.Default);
        }

        /// <summary>
        /// Bulk insert data
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="destinationTableName"></param>
        /// <param name="data"></param>
        internal void BulkInsert<T>(string destinationTableName, IEnumerable<T> data)
        {
            SqlBulkCopy.DestinationTableName = $"[{destinationTableName}]";
            var dt = Common.GetDataTableFromFields(data, SqlBulkCopy);

            SqlBulkCopy.BatchSize = 100000;
            SqlBulkCopy.WriteToServer(dt);
        }
    }
}
