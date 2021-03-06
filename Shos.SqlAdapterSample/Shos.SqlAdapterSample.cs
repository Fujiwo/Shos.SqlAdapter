﻿// .NET Framework 4.5.2
namespace Shos.SqlAdapterSample
{
    using Shos.SqlAdapter;
    using System;
    using System.Configuration;
    using System.Threading.Tasks;

    class Program
    {
        readonly static string connectionString = ConfigurationManager.ConnectionStrings["SqlSampleDatabase"].ConnectionString;

        public class Book
        {
            [Key()]
            public string IsbnCode { get; set; } = "";
            public string Title    { get; set; } = "";
        }

        public class Author
        {
            [ColumnType("smallint")]
            public int    Id   { get; set; }
            [ColumnType("nvarchar(50)")]
            public string Name { get; set; } = "";
        }

        public class Writing
        {
            [Key()]
            [ColumnType("char(10)")]
            public string BookCode { get; set; }
            [Key()]
            public string AuthorId { get; set; }
        }

        static void Test()
        {
            try {
                var adapter = new SqlAdapter { ConnectionString = connectionString };
                var book    = new Book { IsbnCode = "4774180947", Title = "C#プログラマーのための 基礎からわかるLINQマジック!" };
                adapter.Insert(book);
                adapter.Delete(book);
                adapter.Create<Author >();
                adapter.Create<Writing>();
                adapter.Drop  <Author >();
                adapter.Drop  <Writing>();
            }
            catch (Exception ex) {
                Console.WriteLine($"エラー: {ex.Message}");
            }
        }

        static async Task TestAsync()
        {
            try {
                var adapter = new SqlAdapter { ConnectionString = connectionString };
                var book    = new Book { IsbnCode = "4774180947", Title = "C#プログラマーのための 基礎からわかるLINQマジック!" };
                await adapter.InsertAsync(book);
                await adapter.DeleteAsync(book);
                await adapter.CreateAsync<Author >();
                await adapter.CreateAsync<Writing>();
                await adapter.DropAsync  <Author >();
                await adapter.DropAsync  <Writing>();
            }
            catch (Exception ex) {
                Console.WriteLine($"エラー: {ex.Message}");
            }
        }

        static void Main()
        {
            Test();
            TestAsync().Wait();
        }
    }
}
