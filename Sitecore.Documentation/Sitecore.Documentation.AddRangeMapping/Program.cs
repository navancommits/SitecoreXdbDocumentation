using System;
using System.Data.SqlClient;
using System.Linq;
using System.Numerics;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;

namespace Sitecore.Documentation
{
    class Program
    {
        static void Main(string[] args)
        {
            string smmConnectionString = "user id=sa;password=addyoursqlpwd;data source=(local);Initial Catalog=sc1042jul_Xdb.Collection.ShardMapManager;Integrated Security=True;TrustServerCertificate=True";
            string shard1Db = "sc1042jul_Xdb.Collection.Shard1";
            string shard2Db = "sc1042jul_Xdb.Collection.Shard2";
            string shard1ConnStr = $"Data Source=localhost;Initial Catalog={shard1Db};Integrated Security=True";

            Guid shard2MinGuid;
            using (var conn = new SqlConnection(shard1ConnStr))
            {
                conn.Open();

                using (var deleteCmd = new SqlCommand("DELETE FROM __ShardManagement.ShardMappingsLocal WHERE MaxValue IS NULL AND ShardMapId IN (SELECT ShardMapId FROM __ShardManagement.ShardMapsLocal WHERE Name = 'ContactIdShardMap')", conn))
                {
                    int rows = deleteCmd.ExecuteNonQuery();
                    Console.WriteLine($"🧹 Deleted {rows} orphan local mapping(s) with NULL MaxValue in Shard1.");
                }

                using (var cmd = new SqlCommand("SELECT MAX(ContactId) FROM xdb_collection.Contacts", conn))
                {
                    var result = cmd.ExecuteScalar();
                    if (result == DBNull.Value || result == null)
                    {
                        Console.WriteLine("❌ No contacts found in Shard1. Using default midpoint.");
                        BigInteger low = new BigInteger(new Guid("80000000-0000-0000-0000-000000000000").ToByteArray());
                        BigInteger high = new BigInteger(new Guid("FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF").ToByteArray());
                        BigInteger mid = (low + high) / 2;
                        shard2MinGuid = new Guid(mid.ToByteArray().Take(16).ToArray());
                    }
                    else
                    {
                        shard2MinGuid = (Guid)result;
                    }
                    Console.WriteLine($"✅ Max ContactId in Shard1 = {shard2MinGuid}");
                }
            }

            using (var conn = new SqlConnection(smmConnectionString))
            {
                conn.Open();
                var query = @"
                DELETE FROM __ShardManagement.ShardMappingsGlobal
                WHERE MaxValue IS NULL
                  AND ShardMapId IN (
                      SELECT ShardMapId
                      FROM __ShardManagement.ShardMapsGlobal
                      WHERE Name = 'ContactIdShardMap'
                  );";

                using (var cmd = new SqlCommand(query, conn))
                {
                    int rows = cmd.ExecuteNonQuery();
                    Console.WriteLine($"🧹 Deleted {rows} orphan global mapping(s) with NULL MaxValue for Shard1.");
                }
            }

            var smm = ShardMapManagerFactory.GetSqlShardMapManager(smmConnectionString, ShardMapManagerLoadPolicy.Lazy);
            var shardMap = smm.GetRangeShardMap<byte[]>("ContactIdShardMap");

            var shard1Location = new ShardLocation("(local)", shard1Db);
            var shard2Location = new ShardLocation("(local)", shard2Db);

            // Convert shard2MinGuid to 1-byte array if possible (e.g., take first byte)
            byte[] shard2MinFull = shard2MinGuid.ToByteArray();

            // Ensure shard2Min is greater than shard1Low (0x80)
            byte shard2MinByte = (shard2MinFull[0] <= 0x80) ? (byte)(shard2MinFull[0] + 1) : shard2MinFull[0];
            byte[] shard2Min = new byte[] { shard2MinByte };

            //byte[] shard2Min = shard2MinGuid.ToByteArray();
            var shard1Low = new byte[] { 0x80 }; // ✅ Just one byte, exactly 0x80

            Shard shard1 = shardMap.GetShard(shard1Location);
            Shard shard2 = shardMap.GetShard(shard2Location);


            if (CompareBytes(shard1Low, shard2Min) >= 0)
            {
                //var mid = GetMidpoint(shard1Low, new Guid("FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF").ToByteArray());
                //shard2Min = mid;

                var mid = GetMidpoint(shard1Low, new Guid("FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF").ToByteArray());

                // Convert to 1-byte if shard1Low is 1-byte (length check ensures this logic is only applied in 1-byte scenarios)
                if (shard1Low.Length == 1)
                {
                    shard2Min = new byte[] { mid[0] <= shard1Low[0] ? (byte)(shard1Low[0] + 1) : mid[0] };
                }
                else
                {
                    shard2Min = mid;
                }
            }

            //var shard1Range = new Range<byte[]>(shard1Low, shard2Min);

            //var shard1Low = new byte[] { 0x80 };
            var shard1Range = new Range<byte[]>(shard1Low, shard2Min);
            //shardMap.CreateRangeMapping(shard1Range, shard1);

            var allMappings = shardMap.GetMappings().ToList();
            foreach (var m in allMappings)
            {
                if (CompareBytes(m.Value.Low, shard1Range.High) < 0 &&
                    CompareBytes(m.Value.High, shard1Range.Low) > 0 &&
                    m.Shard.Location.Database.Equals(shard1Db, StringComparison.OrdinalIgnoreCase))
                {
                    var offline = shardMap.MarkMappingOffline(m);
                    shardMap.DeleteMapping(offline);
                }
            }

            shardMap.MarkMappingOnline(shardMap.CreateRangeMapping(shard1Range, shard1));

            var shard2Range = new Range<byte[]>(shard2Min, new Guid("FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF").ToByteArray());

            foreach (var m in allMappings)
            {
                if (CompareBytes(m.Value.Low, shard2Range.High) < 0 &&
                    CompareBytes(m.Value.High, shard2Range.Low) > 0 &&
                    m.Shard.Location.Database.Equals(shard2Db, StringComparison.OrdinalIgnoreCase))
                {
                    var offline = shardMap.MarkMappingOffline(m);
                    shardMap.DeleteMapping(offline);
                }
            }

            shardMap.MarkMappingOnline(shardMap.CreateRangeMapping(shard2Range, shard2));

            Console.WriteLine("\n🎉 Shard redistribution completed successfully.");
        }

        static int CompareBytes(byte[] a, byte[] b)
        {
            if (a == null || b == null)
                throw new ArgumentNullException();

            // ❌ Don't resize to 16 bytes anymore
            // if (a.Length != 16) Array.Resize(ref a, 16);
            // if (b.Length != 16) Array.Resize(ref b, 16);

            int len = Math.Min(a.Length, b.Length);
            for (int i = 0; i < len; i++)
            {
                int diff = a[i].CompareTo(b[i]);
                if (diff != 0)
                    return diff;
            }

            // Longer array wins if all prior bytes are equal
            return a.Length.CompareTo(b.Length);
        }


        static byte[] GetMidpoint(byte[] low, byte[] high)
        {
            BigInteger lowInt = new BigInteger(low.Concat(new byte[] { 0 }).ToArray());
            BigInteger highInt = new BigInteger(high.Concat(new byte[] { 0 }).ToArray());
            BigInteger mid = (lowInt + highInt) / 2;
            var midBytes = mid.ToByteArray().Take(16).ToArray();
            Array.Resize(ref midBytes, 16);
            return midBytes;
        }
    }
}