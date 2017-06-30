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

    static class ReflectionExtension
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

            public bool Create(SqlConnection connection)
            {
                var partSql = this.Select(culumn => $"{culumn.Name} {culumn.TypeName}").Connect(", ");
                var sql     = $"CREATE TABLE {Name} ({partSql})";
                using (var command = new SqlCommand(sql, connection)) {
                    connection.Open();
                    return command.ExecuteNonQuery() > 0;
                }
            }

            public bool Drop(SqlConnection connection)
            {
                var sql = $"DROP TABLE {Name}";
                using (var command = new SqlCommand(sql, connection)) {
                    connection.Open();
                    return command.ExecuteNonQuery() > 0;
                }
            }

            public bool Insert(SqlConnection connection)
            {
                var sql = $"INSERT INTO {Name} ({this.Select(culumn => culumn.Name).Connect(", ")}) VALUES ({this.Select(culumn => culumn.ParameterName).Connect(", ")})";
                var parameters = this.Select(culumn => new SqlParameter(culumn.ParameterName, culumn.ParameterValue));

                using (var command = new SqlCommand(sql, connection)) {
                    command.Parameters.AddRange(parameters.ToArray());
                    connection.Open();
                    return command.ExecuteNonQuery() > 0;
                }
            }

            public bool Delete(SqlConnection connection)
            {
                var keyColumns = this.Where(column => column.IsKey);
                if (keyColumns.Count() <= 0)
                    return false;
                var partSql    = KeyColumns.Select(keyColumn => $"{keyColumn.Name} = { keyColumn.ParameterName}").Connect(" AND ");
                var sql        = $"DELETE FROM {Name} WHERE {partSql}";
                var parameters = KeyColumns.Select(keyColumn => new SqlParameter(keyColumn.ParameterName, keyColumn.ParameterValue));

                using (var command = new SqlCommand(sql, connection)) {
                    command.Parameters.AddRange(parameters.ToArray());
                    connection.Open();
                    return command.ExecuteNonQuery() > 0;
                }
            }

            public void Add(Column column) => columnList.Add(column);

            public IEnumerator<Column> GetEnumerator() => columnList.GetEnumerator();

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        }

        public string ConnectionString { get; set; }

        /*
        CREATE TABLE CardInfo (
          CardID nchar(6),
          CustomerID nchar(5),
          IssueDate datetime,
          ExpireDate datetime,
          EmployeeID int
        )
         */

        public bool Create<ItemType>()
        {
            using (var connection = new SqlConnection(ConnectionString))
                return new Table(typeof(ItemType)).Create(connection);
        }

        public bool Drop<ItemType>()
        {
            using (var connection = new SqlConnection(ConnectionString))
                return new Table(typeof(ItemType)).Drop(connection);
        }

        public bool Insert(object item)
        {
            using (var connection = new SqlConnection(ConnectionString))
                return new Table(item).Insert(connection);
        }

        public bool Delete(object item)
        {
            using (var connection = new SqlConnection(ConnectionString))
                return new Table(item).Delete(connection);
        }
    }
}
