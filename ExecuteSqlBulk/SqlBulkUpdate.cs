using System;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;
using System.Linq;
using System.Text;

namespace ExecuteSqlBulk
{
    internal class SqlBulkUpdate : SqlBulkBase
    {
        internal SqlBulkUpdate(SqlConnection connection, SqlTransaction tran)
        {
            SqlBulk(connection, tran, SqlBulkCopyOptions.KeepIdentity);
        }

        /// <summary>
        /// Bulk update data
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="destinationTableName">Table name</param>
        /// <param name="data">Data</param>
        /// <param name="pkColumns">Primary key</param>
        /// <param name="updateColumns">Columns to update</param>
        internal int BulkUpdate<T>(string destinationTableName, IEnumerable<T> data, List<string> pkColumns, List<string> updateColumns)
        {
            var tempTablename = "#" + destinationTableName + "_" + Guid.NewGuid().ToString("N");

            var cols = new List<string>();
            cols.AddRange(pkColumns);
            cols.AddRange(updateColumns);
            var allColumnNames = cols.Distinct(StringComparer.OrdinalIgnoreCase).ToList();

            // Create temporary table
            CreateTempTable(destinationTableName, tempTablename, allColumnNames);

            // Copy data into temporary table
            var dataAsArray = data as T[] ?? data.ToArray();
            SqlBulkCopy.DestinationTableName = tempTablename;
            var dt = Common.GetDataTableFromFields(dataAsArray, SqlBulkCopy, allColumnNames);
            SqlBulkCopy.BatchSize = 100000;

            SqlBulkCopy.WriteToServer(dt);
            // Merge data from temporary table into destination table
            var row = MergeTempAndDestination(destinationTableName, tempTablename, pkColumns, updateColumns);
            // Drop temporary table
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

        private int MergeTempAndDestination(string destinationTableName, string tempTablename, List<string> pkColumns, List<string> updateColumns)
        {
            var pkSql = new StringBuilder();
            for (var i = 0; i < pkColumns.Count; i++)
            {
                if (i > 0)
                {
                    pkSql.Append(" AND");
                }
                pkSql.Append($" Target.[{pkColumns[i]}]=Source.[{pkColumns[i]}]");
            }

            var updateSql = new StringBuilder();
            for (var i = 0; i < updateColumns.Count; i++)
            {
                if (i > 0)
                {
                    updateSql.Append(",");
                }
                updateSql.Append($"Target.[{updateColumns[i]}]=Source.[{updateColumns[i]}]");
            }
            var mergeSql = $"MERGE INTO [{destinationTableName}] AS Target USING [{tempTablename}] AS Source ON {pkSql} WHEN MATCHED THEN UPDATE SET {updateSql};";

            var cmd = Connection.CreateCommand();
            cmd.CommandText = mergeSql;
            cmd.Transaction = Tran;
            return cmd.ExecuteNonQuery();
        }

        /// <summary>
        /// Create a temporary table
        /// </summary>
        /// <param name="destinationTableName"></param>
        /// <param name="tempTablename"></param>
        /// <param name="colomns"></param>
        private void CreateTempTable(string destinationTableName, string tempTablename, List<string> colomns)
        {
            var str = colomns.Count == 0 ? "*" : string.Join(",", colomns.Select(p => $"[{p}]"));
            var cmd = Connection.CreateCommand();
            cmd.CommandText = $"SELECT TOP 0 {str} INTO [{tempTablename}] FROM [{destinationTableName}];";
            cmd.Transaction = Tran;
            cmd.ExecuteNonQuery();
        }
    }
}
