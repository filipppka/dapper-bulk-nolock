using Dapper;
using FastMember;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BulkMerge
{
    public static class DapperExtensions
    {
        public static async Task<IEnumerable<T>> BulkMergeAsync<T>(this IDbConnection connection, IEnumerable<T> list, string detinationTable = null)
        {
            detinationTable = detinationTable is null ? typeof(T).Name : detinationTable;

            var tempTableName = $"{detinationTable}_{Guid.NewGuid().ToString("N")}";

            var identityInfo = await connection.QueryFirstOrDefaultAsync<(string ColumnName, string Type)>($"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1 AND TABLE_NAME = '{detinationTable}' AND TABLE_SCHEMA = 'dbo'");

            var identityExists = !string.IsNullOrEmpty(identityInfo.ColumnName);
            var primaryKeysInfo = await connection.QueryAsync<string>($"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)), 'IsPrimaryKey') = 1 AND TABLE_NAME = '{detinationTable}' AND TABLE_SCHEMA = 'dbo'");
            if (!primaryKeysInfo.Any())
                throw new Exception("NO PRIMARY KEYS");
            var typeAccessor = TypeAccessor.Create(typeof(T));
            if (identityExists && !typeAccessor.GetMembers().Any(x => x.Name != identityInfo.ColumnName))
                throw new Exception("NO IDENTITY PROPERTY");

            var tempTable = await connection.ExecuteAsync($"SELECT * INTO {tempTableName} FROM {detinationTable} WHERE 1 = 0");

            if (identityExists)
            {
                await connection.ExecuteAsync($"ALTER TABLE {tempTableName} DROP COLUMN {identityInfo.ColumnName}");
                await connection.ExecuteAsync($"ALTER TABLE {tempTableName} ADD {identityInfo.ColumnName} {identityInfo.Type}");
            }

            var sqlConnection = connection as SqlConnection;
            await sqlConnection.OpenAsync();
            var colmunNames = typeAccessor.GetMembers().Select(x => x.Name).ToArray();
            using (var bcp = new SqlBulkCopy(sqlConnection))
            using (var reader = ObjectReader.Create(list))
            {
                foreach (var columName in colmunNames)
                {
                    bcp.ColumnMappings.Add(new SqlBulkCopyColumnMapping(columName, columName));
                }
                bcp.DestinationTableName = tempTableName;
                await bcp.WriteToServerAsync(reader);
            }
            await sqlConnection.CloseAsync();
            var merge = CreateMergeStatement(primaryKeysInfo.ToList(), tempTableName, detinationTable, identityInfo, typeAccessor);
            var result = await connection.QueryAsync<long>(merge);
            await connection.QueryAsync($"DROP TABLE {tempTableName}");
            return list;
        }

        private static string CreateMergeStatement(IList<string> primaryKeys, string tempTable, string targetTable, (string ColumnName, string Type) identityInfo,
            TypeAccessor typeAccessor)
        {
            var members = typeAccessor.GetMembers().Where(x => x.Name != identityInfo.ColumnName);
            var stringBuilder = new StringBuilder();
            var identityExist = !string.IsNullOrEmpty(identityInfo.ColumnName);

            if (identityExist)
            {
                stringBuilder.Append($"DECLARE @OutputIdentity TABLE ([Id] {identityInfo.Type})");
            }
            stringBuilder.Append($@"
MERGE [{targetTable}] AS T  
USING (SELECT * FROM [{tempTable}]) AS S
    ON ({string.Join(",", primaryKeys.Select(x => $"S.[{x}] = T.[{x}]"))})
WHEN NOT MATCHED
	THEN INSERT ({string.Join(",", members.Select(x => $"[{x.Name}]"))}) VALUES ({string.Join(",", members.Select(x => $"S.[{x.Name}]"))})
WHEN MATCHED 
	THEN UPDATE SET {string.Join(",", members.Select(x => $"T.[{x.Name}] = S.[{x.Name}]"))}");
            if (identityExist)
            {
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"OUTPUT inserted.{identityInfo.ColumnName} INTO @OutputIdentity;");
                stringBuilder.AppendLine("SELECT * FROM @OutputIdentity ORDER BY [Id] ASC");
            }
            else
            {
                stringBuilder.Append(";");
            }
            return stringBuilder.ToString();
        }
    }
}
