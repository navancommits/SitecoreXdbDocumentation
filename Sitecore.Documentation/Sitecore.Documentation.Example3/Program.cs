using System;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace GuidShardRangeChecker
{
    class Program
    {
        static void Main(string[] args)
        {
            //string smmConnectionString = "Data Source=localhost;Initial Catalog=ShardMapManager;Integrated Security=True";
            string smmConnectionString = "user id=sa;password=xxxx;data source=(local);Initial Catalog=sc104gh_Xdb.Collection.ShardMapManager;Integrated Security=True;TrustServerCertificate=True";

            Console.Write("Enter a GUID to test (or press Enter to generate a random one): ");
            string input = Console.ReadLine();
            Guid testGuid = string.IsNullOrWhiteSpace(input) ? Guid.NewGuid() : Guid.Parse(input);
            byte[] testGuidBytes = testGuid.ToByteArray();

            Console.WriteLine($"\n🔎 Testing GUID: {testGuid}");

            List<(string ShardName, byte[] Min, byte[] Max)> shardRanges = GetShardRanges(smmConnectionString);

            foreach (var shard in shardRanges)
            {
                bool inRange = CompareBytes(testGuidBytes, shard.Min) >= 0 && CompareBytes(testGuidBytes, shard.Max) < 0;
                Console.WriteLine($"\n🔹 Shard: {shard.ShardName}");
                Console.WriteLine($"   Min: {new Guid(PadTo16Bytes(shard.Min))}  ({BitConverter.ToString(shard.Min)})");
                Console.WriteLine($"   Max: {new Guid(PadTo16Bytes(shard.Max))}  ({BitConverter.ToString(shard.Max)})");
                Console.WriteLine(inRange
                    ? "✅ GUID falls within this shard range."
                    : "❌ GUID does NOT fall within this shard range.");
            }
        }

        static List<(string ShardName, byte[] Min, byte[] Max)> GetShardRanges(string connStr)
        {
            var ranges = new List<(string, byte[], byte[])>();
            using (var conn = new SqlConnection(connStr))
            {
                conn.Open();
                var cmd = new SqlCommand(@"
                SELECT sg.DatabaseName, sm.MinValue, sm.MaxValue
                FROM __ShardManagement.ShardMappingsGlobal sm
                JOIN __ShardManagement.ShardsGlobal sg ON sm.ShardId = sg.ShardId
                JOIN __ShardManagement.ShardMapsGlobal map ON sm.ShardMapId = map.ShardMapId
                WHERE map.Name = 'ContactIdShardMap'
                ", conn);

                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string dbName = reader.GetString(0);
                        byte[] min = (byte[])reader[1];
                        byte[] max;
                        if (reader.IsDBNull(2))
                        {
                            max = new Guid("FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF").ToByteArray();
                        }
                        else
                        {
                            max = (byte[])reader[2];
                        }
                        ranges.Add((dbName, min, max));
                    }
                }
            }
            return ranges;
        }

        static int CompareBytes(byte[] a, byte[] b)
        {
            if (a == null || b == null) throw new ArgumentNullException();

            byte[] a16 = PadTo16Bytes(a);
            byte[] b16 = PadTo16Bytes(b);

            for (int i = 0; i < 16; i++)
            {
                int cmp = a16[i].CompareTo(b16[i]);
                if (cmp != 0) return cmp;
            }
            return 0;
        }

        static byte[] PadTo16Bytes(byte[] source)
        {
            if (source.Length == 16) return source;
            byte[] padded = new byte[16];
            Array.Copy(source, 0, padded, 0, source.Length);
            return padded;
        }
    }
}