using System;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;
using System.Linq;
using System.Text;

namespace ExecuteSqlBulk
{
    internal class SqlBulkDelete : SqlBulkBase
    {
        internal SqlBulkDelete(SqlConnection connection, SqlTransaction tran)
        {
            SqlBulk(connection, tran, SqlBulkCopyOptions.KeepIdentity);
        }

        /// <summary>
        /// Delete all data from the table
        /// </summary>
        /// <param name="destinationTableName"></param>
        internal void BulkDelete(string destinationTableName)
        {
            var cmd = Connection.CreateCommand();
            cmd.CommandText = $"TRUNCATE TABLE [{destinationTableName}];";
            cmd.Transaction = Tran;
            cmd.ExecuteNonQuery();
        }

        /// <summary>
        /// Bulk delete data
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="destinationTableName">Table name</param>
        /// <param name="data">Data</param>
        /// <param name="columnNameToMatchs">Primary key</param>
        internal int BulkDelete<T>(string destinationTableName, IEnumerable<T> data, List<string> columnNameToMatchs)
        {
            var tempTablename = "#" + destinationTableName + "_" + Guid.NewGuid().ToString("N");
            //
            CreateTempTable(destinationTableName, tempTablename);
            //
            var dataAsArray = data as T[] ?? data.ToArray();
            SqlBulkCopy.DestinationTableName = tempTablename;
            var dt = Common.GetDataTableFromFields(dataAsArray, SqlBulkCopy);
            SqlBulkCopy.BatchSize = 100000;
            SqlBulkCopy.WriteToServer(dt);
            //
            var row = DeleteTempAndDestination(destinationTableName, tempTablename, columnNameToMatchs);
            //
            DropTempTable(tempTablename);

            return row;
        }

        private void DropTempTable(string tempTablename)
        {
            var cmd = Connection.CreateCommand();
            cmd.CommandText = $"DROP TABLE [{tempTablename}]";
            cmd.Transaction = Tran;
            cmd.ExecuteNonQuery();
        }

        private int DeleteTempAndDestination(string destinationTableName, string tempTablename,
            List<string> columnNameToMatchs)
        {
            var sb = new StringBuilder(columnNameToMatchs.Count > 0 ? " WHERE" : "");
            for (var i = 0; i < columnNameToMatchs.Count; i++)
            {
                if (i > 0)
                {
                    sb.Append(" AND");
                }

                sb.Append($" t1.[{columnNameToMatchs[i]}]=t2.[{columnNameToMatchs[i]}]");
            }
            var deleteSql = $"DELETE [{destinationTableName}] FROM [{destinationTableName}] t1,{tempTablename} t2{sb};";

            var cmd = Connection.CreateCommand();
            cmd.CommandText = deleteSql;
            cmd.Transaction = Tran;
            return cmd.ExecuteNonQuery();
        }

        private void CreateTempTable(string destinationTableName, string tempTablename)
        {
            var cmd = Connection.CreateCommand();
            cmd.CommandText = $"SELECT TOP 0 * INTO [{tempTablename}] FROM [{destinationTableName}];";
            cmd.Transaction = Tran;
            cmd.ExecuteNonQuery();
        }
    }
}
