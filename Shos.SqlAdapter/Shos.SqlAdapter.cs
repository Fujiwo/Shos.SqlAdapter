// .NET Framework 4.5.2
namespace Shos.SqlAdapter
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlClient;
    using System.Linq;
    using System.Reflection;
    using System.Text;
    using System.Threading.Tasks;

    static class EnumerableExtension
    {
        public static void ForEach<TElement>(this IEnumerable<TElement> @this, Action<int, TElement> action)
        {
            var index = 0;
            foreach (var element in @this)
                action(index++, element);
        }

        /// <summary>文字列のシーケンスを連結する。</summary>
        /// <param name="this">連結したい文字列のシーケンス</param>
        /// <param name="separator">間に挟む文字列 (デフォルトは空文字列)</param>
        /// <returns>連結後の文字列</returns>
        /// <example>
        /// var connectedText = new [] { "ABC", "DE", "FGHI" }.Connect();
        /// 結果: connectedText の値は "ABCDEFGHI"
        /// var connectedText = new [] { "ABC", "DE", "FGHI" }.Connect(", ");
        /// 結果: connectedText の値は "ABC, DE, FGHI"
        /// </example>
        public static string Connect(this IEnumerable<string> @this, string separator = "")
        {
            var stringBuilder = new StringBuilder();
            @this.ForEach((index, text) => {
                if (index++ != 0)
                    stringBuilder.Append(separator);
                stringBuilder.Append(text);
            });
            return stringBuilder.ToString();
        }
    }

    static class SqlAdapterExtension
    {
        public static IEnumerable<PropertyInfo> Keys(this IEnumerable<PropertyInfo> properties, string typeName)
        {
            var keyProperties = properties.Where(property => property.GetCustomAttributes(typeof(KeyAttribute)).Count() > 0);
            return keyProperties.Count() > 0
                   ? keyProperties
                   : properties.Where(property => property.Name.ToLower().Equals("id") || property.Name.ToLower().Equals(typeName + "id"));
        }

        public static IEnumerable<PropertyInfo> Keys(this object item)
        {
            var itemType = item.GetType();
            return itemType.GetProperties().Keys(itemType.Name);
        }

        public static string TypeName(this PropertyInfo property)
            => ((ColumnTypeAttribute)(property.GetCustomAttributes(typeof(ColumnTypeAttribute)).SingleOrDefault()))?.ColumnTypeName;

        public static bool NonQuery(this IDbCommand command)
        {
            if (command == null)
                return false;
            command.Connection?.Open();
            return command.ExecuteNonQuery() > 0;
        }

        public static async Task<bool> NonQueryAsync(this SqlCommand command)
        {
            if (command == null)
                return false;
            await command.Connection?.OpenAsync();
            return await command.ExecuteNonQueryAsync() > 0;
        }

        public static bool NonQuery(this SqlConnection connection, Func<SqlConnection, SqlCommand> commandFactoryMethod)
        {
            using (var command = commandFactoryMethod(connection))
                return command.NonQuery();
        }

        public static async Task<bool> NonQueryAsync(this SqlConnection connection, Func<SqlConnection, SqlCommand> commandFactoryMethod)
        {
            using (var command = commandFactoryMethod(connection))
                return await command.NonQueryAsync();
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class KeyAttribute : Attribute
    {}

    [AttributeUsage(AttributeTargets.Property)]
    public class ColumnTypeAttribute : Attribute
    {
        public string ColumnTypeName { get; private set; }

        public ColumnTypeAttribute(string columnTypeName) => ColumnTypeName = columnTypeName;
    }

    public class SqlAdapter
    {
        class Column
        {
            string typeName = null;

            public string Name           { get; set; }
            public string ParameterName => "@" + Name;
            public object ParameterValue { get; set; }
            public Type   ParameterType  { get; set; }
            public string TypeName       {
                get { return string.IsNullOrWhiteSpace(typeName) ? ParameterTypeNameToDatabaseTypeName(ParameterType.Name) : typeName; }
                set { typeName = value; }
            }
            public bool   IsKey          { get; set; }

            static string ParameterTypeNameToDatabaseTypeName(string parameterTypeName)
            {
                switch (parameterTypeName) {
                    case "Boolean" : return "bit"          ;
                    case "Int8"    : return "tinyint"      ;
                    case "Int16"   : return "smallint"     ;
                    case "Int32"   : return "int"          ;
                    case "Int64"   : return "bigint"       ;
                    case "Decimal" : return "decimal"      ;
                    case "Double"  : return "float"        ;
                    case "String"  : return "nvarchar(MAX)";
                    case "DateTime": return "datetime"     ;
                }
                return parameterTypeName;
            }
        };

        class Table : IEnumerable<Column>
        {
            readonly IList<Column> columnList;
            IEnumerable<Column> KeyColumns => columnList.Where(column => column.IsKey);

            public string Name { get; set; }

            public Table(object item)
            {
                var itemType      = item.GetType();
                Name              = itemType.Name;
                var properties    = itemType.GetProperties();
                var keyProperties = properties.Keys(Name);
                columnList        = properties.Select(property => new Column { Name = property.Name, ParameterValue = property.GetValue(item), ParameterType = property.PropertyType, TypeName = property.TypeName(), IsKey = keyProperties.Contains(property) }).ToList();
            }

            public Table(Type type)
            {
                Name              = type.Name;
                var properties    = type.GetProperties();
                var keyProperties = properties.Keys(Name);
                columnList        = properties.Select(property => new Column { Name = property.Name, ParameterValue = null, ParameterType = property.PropertyType, TypeName = property.TypeName(), IsKey = keyProperties.Contains(property) }).ToList();
            }

            public bool             Create     (SqlConnection connection) =>       connection.NonQuery     (CreateCommand);
            public async Task<bool> CreateAsync(SqlConnection connection) => await connection.NonQueryAsync(CreateCommand);
            public bool             Drop       (SqlConnection connection) =>       connection.NonQuery     (DropCommand  );
            public async Task<bool> DropAsync  (SqlConnection connection) => await connection.NonQueryAsync(DropCommand  );
            public bool             Insert     (SqlConnection connection) =>       connection.NonQuery     (InsertCommand);
            public async Task<bool> InsertAsync(SqlConnection connection) => await connection.NonQueryAsync(InsertCommand);
            public bool             Delete     (SqlConnection connection) =>       connection.NonQuery     (DeleteCommand);
            public async Task<bool> DeleteAsync(SqlConnection connection) => await connection.NonQueryAsync(DeleteCommand);

            public void Add(Column column) => columnList.Add(column);

            public IEnumerator<Column> GetEnumerator() => columnList.GetEnumerator();

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

            SqlCommand CreateCommand(SqlConnection connection)
            {
                var partSql = this.Select(culumn => $"{culumn.Name} {culumn.TypeName}").Connect(", ");
                var sql     = $"CREATE TABLE {Name} ({partSql})";
                return new SqlCommand(sql, connection);
            }

            SqlCommand DropCommand(SqlConnection connection)
            {
                var sql = $"DROP TABLE {Name}";
                return new SqlCommand(sql, connection);
            }

            SqlCommand InsertCommand(SqlConnection connection)
            {
                var sql        = $"INSERT INTO {Name} ({this.Select(culumn => culumn.Name).Connect(", ")}) VALUES ({this.Select(culumn => culumn.ParameterName).Connect(", ")})";
                var parameters = this.Select(culumn => new SqlParameter(culumn.ParameterName, culumn.ParameterValue));
                var command    = new SqlCommand(sql, connection);
                command.Parameters.AddRange(parameters.ToArray());
                return command;
            }

            SqlCommand DeleteCommand(SqlConnection connection)
            {
                var keyColumns = this.Where(column => column.IsKey);
                if (keyColumns.Count() <= 0)
                    return null;
                var partSql    = KeyColumns.Select(keyColumn => $"{keyColumn.Name} = { keyColumn.ParameterName}").Connect(" AND ");
                var sql        = $"DELETE FROM {Name} WHERE {partSql}";
                var parameters = KeyColumns.Select(keyColumn => new SqlParameter(keyColumn.ParameterName, keyColumn.ParameterValue));
                var command    = new SqlCommand(sql, connection);
                command.Parameters.AddRange(parameters.ToArray());
                return command;
            }
        }

        public string ConnectionString { get; set; }

        public bool Create<ItemType>()
        {
            using (var connection = new SqlConnection(ConnectionString))
                return new Table(typeof(ItemType)).Create(connection);
        }

        public async Task<bool> CreateAsync<ItemType>()
        {
            using (var connection = new SqlConnection(ConnectionString))
                return await new Table(typeof(ItemType)).CreateAsync(connection);
        }

        public bool Drop<ItemType>()
        {
            using (var connection = new SqlConnection(ConnectionString))
                return new Table(typeof(ItemType)).Drop(connection);
        }

        public async Task<bool> DropAsync<ItemType>()
        {
            using (var connection = new SqlConnection(ConnectionString))
                return await new Table(typeof(ItemType)).DropAsync(connection);
        }

        public bool Insert(object item)
        {
            using (var connection = new SqlConnection(ConnectionString))
                return new Table(item).Insert(connection);
        }

        public async Task<bool> InsertAsync(object item)
        {
            using (var connection = new SqlConnection(ConnectionString))
                return await new Table(item).InsertAsync(connection);
        }

        public bool Delete(object item)
        {
            using (var connection = new SqlConnection(ConnectionString))
                return new Table(item).Delete(connection);
        }

        public async Task<bool> DeleteAsync(object item)
        {
            using (var connection = new SqlConnection(ConnectionString))
                return await new Table(item).DeleteAsync(connection);
        }
    }
}
