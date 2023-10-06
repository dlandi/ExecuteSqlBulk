using System;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;

namespace ExecuteSqlBulk
{
    /// <summary>
    /// Extensions
    /// </summary>
    public static class SqlBulkExt
    {
        /// <summary>
        /// Bulk insert data (supports NotMapped attribute)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="db"></param>
        /// <param name="dt"></param>
        /// <param name="tran"></param>
        public static void BulkInsert<T>(this SqlConnection db, List<T> dt, SqlTransaction tran = null)
        {
            var tableName = typeof(T).Name;
            BulkInsert(db, tableName, dt, tran);
        }

        /// <summary>
        /// Bulk Insert with a given destination name
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="db"></param>
        /// <param name="tableName"></param>
        /// <param name="dt"></param>
        /// <param name="tran"></param>
        public static void BulkInsert<T>(this SqlConnection db, string tableName, List<T> dt, SqlTransaction tran = null)
        {
            using (var sbc = new SqlBulkInsert(db, tran))
            {
                sbc.BulkInsert(tableName, dt);
            }
        }

        /// <summary>
        /// Bulk update data (supports NotMapped attribute)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="TUpdateColumn"></typeparam>
        /// <typeparam name="TPkColumn"></typeparam>
        /// <param name="db"></param>
        /// <param name="dt"></param>
        /// <param name="columnUpdateExpression">Update column collection</param>
        /// <param name="columnPrimaryKeyExpression">Primary key column</param>
        /// <param name="tran"></param>
        /// <returns>Affected rows</returns>
        public static int BulkUpdate<T, TUpdateColumn, TPkColumn>(this SqlConnection db, List<T> dt, Expression<Func<T, TUpdateColumn>> columnUpdateExpression, Expression<Func<T, TPkColumn>> columnPrimaryKeyExpression, SqlTransaction tran = null) where T : new()
        {
            var tableName = typeof(T).Name;
            return BulkUpdate(db, tableName, dt, columnUpdateExpression, columnPrimaryKeyExpression, tran);
        }

        /// <summary>
        /// Bulk update data (supports NotMapped attribute)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="TUpdateColumn"></typeparam>
        /// <typeparam name="TPkColumn"></typeparam>
        /// <param name="db"></param>
        /// <param name="tableName"></param>
        /// <param name="dt"></param>
        /// <param name="columnUpdateExpression">Update column collection</param>
        /// <param name="columnPrimaryKeyExpression">Primary key column</param>
        /// <param name="tran"></param>
        /// <returns>Affected rows</returns>
        public static int BulkUpdate<T, TUpdateColumn, TPkColumn>(this SqlConnection db, string tableName, List<T> dt, Expression<Func<T, TUpdateColumn>> columnUpdateExpression, Expression<Func<T, TPkColumn>> columnPrimaryKeyExpression, SqlTransaction tran = null) where T : new()
        {
            if (columnPrimaryKeyExpression == null)
            {
                throw new Exception("columnPrimaryKeyExpression cannot be null");
            }
            if (columnUpdateExpression == null)
            {
                throw new Exception("columnInputExpression cannot be null");
            }

            var pkColumns = GetColumns(columnPrimaryKeyExpression);
            if (pkColumns.Count == 0)
            {
                throw new Exception("Primary key cannot be null");
            }

            var updateColumns = GetColumns(columnUpdateExpression);
            if (updateColumns.Count == 0)
            {
                throw new Exception("Update columns cannot be null");
            }

            using (var sbu = new SqlBulkUpdate(db, tran))
            {
                return sbu.BulkUpdate(tableName, dt, pkColumns, updateColumns);
            }
        }

        /// <summary>
        /// Get columns
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="TColumn"></typeparam>
        /// <param name="expression"></param>
        /// <returns></returns>
        internal static List<string> GetColumns<T, TColumn>(this Expression<Func<T, TColumn>> expression) where T : new()
        {
            if (expression.Body is MemberExpression memberBody)
            {
                return new List<string>() { memberBody.Member.Name };
            }

            if (expression.Body is UnaryExpression unaryBody)
            {
                var name = ((MemberExpression)unaryBody.Operand).Member.Name;
                return new List<string>() { name };
            }

            if (expression.Body is ParameterExpression parameterBody)
            {
                return new List<string>() { parameterBody.Type.Name };
            }

            var t = new T();
            var obj = expression.Compile().Invoke(t);
            var cols = Common.GetColumns(obj);
            return cols.ToList();
        }

        /// <summary>
        /// Bulk delete data (supports NotMapped attribute)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="TPk"></typeparam>
        /// <param name="db"></param>
        /// <param name="dt"></param>
        /// <param name="columnPrimaryKeyExpression"></param>
        /// <param name="tran"></param>
        /// <returns>Affected rows</returns>
        public static int BulkDelete<T, TPk>(this SqlConnection db, List<T> dt, Expression<Func<T, TPk>> columnPrimaryKeyExpression, SqlTransaction tran = null) where T : new()
        {
            if (columnPrimaryKeyExpression == null)
            {
                throw new Exception("columnPrimaryKeyExpression cannot be null");
            }

            var pkColumns = GetColumns(columnPrimaryKeyExpression);
            if (pkColumns.Count == 0)
            {
                throw new Exception("Primary key cannot be null");
            }

            var tableName = typeof(T).Name;
            using (var sbc = new SqlBulkDelete(db, tran))
            {
                return sbc.BulkDelete(tableName, dt, pkColumns);
            }
        }

        /// <summary>
        /// Delete all data from the table
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="db"></param>
        /// <param name="tran"></param>
        public static void BulkDelete<T>(this SqlConnection db, SqlTransaction tran = null)
        {
            var tableName = typeof(T).Name;
            BulkDelete(db, tableName, tran);
        }

        /// <summary>
        /// Bulk Delete with given table name
        /// </summary>
        /// <param name="db"></param>
        /// <param name="tableName"></param>
        /// <param name="tran"></param>
        public static void BulkDelete(this SqlConnection db, string tableName, SqlTransaction tran = null)
        {
            using (var sbc = new SqlBulkDelete(db, tran))
            {
                sbc.BulkDelete(tableName);
            }
        }
    }
}
