using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;
using System.Threading.Tasks;
using Xunit;
namespace BulkMerge.Tests
{
    public class BulkMergeItem
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public decimal Amount { get; set; }
    }

    public class BulkMegeTests
    {
        [Fact]
        public async Task SqlBulkCopyTest()
        {
            try
            {
                var list = new List<BulkMergeItem>();
                for (var i = 0; i < 100000; i++)
                {
                    list.Add(new BulkMergeItem { Amount = i, Name = $"Name{i}" });
                }

                await using var connection = new SqlConnection("Server=DESKTOP-22FGMF9\\SQLEXPRESS;Database=test;Trusted_Connection=True;");
                await connection.BulkMergeAsync("BulkMergeTest", list);
            }
            catch (Exception e)
            {
                ;
            }
            
        }
    }
}
