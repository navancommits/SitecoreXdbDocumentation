using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;
using System.Xml.Linq;

class Program
{
    static string smmConnString = ConfigurationManager.AppSettings["smmConnString"];
    static string dbPwd = ConfigurationManager.AppSettings["DBPassword"];
    static string dbUser = ConfigurationManager.AppSettings["DBUser"];
    static string sqlAdminPwd = ConfigurationManager.AppSettings["SqlAdminPassword"];
    static string sqlAdminUser = ConfigurationManager.AppSettings["SqlAdminUser"];
    static string shardMapManagerDBName = ConfigurationManager.AppSettings["smmDbName"];
    static string toolPath = ConfigurationManager.AppSettings["shardDeploymentToolPath"];
    static string server = ConfigurationManager.AppSettings["shardServer"];
    static string sourceDbName = ConfigurationManager.AppSettings["sourceShard"];
    static string targetDbName = ConfigurationManager.AppSettings["targetShard"];
    static string shardPrefix;
    static string shardMapName = "ContactIdShardMap"; //ConfigurationManager.AppSettings["shardMapName"];
    static void Main()
    {
        ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(smmConnString, ShardMapManagerLoadPolicy.Lazy);
        Console.Write("\nEnter Shard Option (1/2): ");
        int shardOption = int.Parse(Console.ReadLine());

        switch (shardOption)
        {
            case 1:
                //Split when no data
                AddShardtoDefaultSetup(smm);
                break;
            case 2:
                GetInputs();
                //Pre-requisite: Target Shard db must be present with schema
                SetupDBwithSchema(targetDbName);
                //Split when there is data
                ShardExistingSetup(smm);
                break;
            default:
                break;
        }
    }

    static void GetInputs()
    {

        Console.Write("Enter shard server name: ");
        server = Console.ReadLine();

        Console.Write("Provide Source Shard DB Name: ");
        sourceDbName = Console.ReadLine();

        Console.Write("Provide Target Shard DB Name: ");
        targetDbName = Console.ReadLine();

        Console.Write("Enter shard name prefix (e.g., sc104k_Xdb.Collection.Shard): ");
        shardPrefix = Console.ReadLine();
    }

    static void ShardExistingSetup(ShardMapManager smm)
    {
        var shardMap1 = smm.GetRangeShardMap<byte[]>("ContactIdShardMap");
        var shardMap2 = smm.GetRangeShardMap<byte[]>("ContactIdentifiersIndexShardMap");
        var shardMap3 = smm.GetRangeShardMap<byte[]>("DeviceProfileIdShardMap");

        var sourceLocation = new ShardLocation(server, sourceDbName);
        var targetLocation = new ShardLocation(server, targetDbName);

        Console.Write("Enter new Source hex range min value: ");
        string startSrcHex = Console.ReadLine().Replace("0x", "");

        Console.Write("Enter new Source hex range max value: ");
        string endSrcHex = Console.ReadLine().Replace("0x", "");

        Console.Write("Enter Target hex range min value: ");
        string startTarHex = Console.ReadLine().Replace("0x", "");

        Console.Write("Enter Target hex range max value: ");
        string endTarHex = Console.ReadLine().Replace("0x", "");

        // Parse hex strings into bytes
        byte startByte = byte.Parse(startTarHex, NumberStyles.HexNumber);
        byte endByte = byte.Parse(endTarHex, NumberStyles.HexNumber);

        // Convert to single-byte arrays
        byte[] rangeStart = new byte[] { startByte };
        byte[] rangeEnd = new byte[] { endByte };

        // Debug output
        Console.WriteLine($"Range Start: 0x{rangeStart[0]:X2}");
        Console.WriteLine($"Range End:   0x{rangeEnd[0]:X2}");

        // Parse hex strings into bytes
        byte startDelByte = byte.Parse(startSrcHex, NumberStyles.HexNumber);
        byte endDelByte = byte.Parse(endTarHex, NumberStyles.HexNumber);

        // Convert to single-byte arrays
        byte[] delRangeStart = new byte[] { startDelByte };
        byte[] delRangeEnd = new byte[] { endDelByte };

        DeleteExistingGlobalMappings(delRangeStart);
        DeleteSrcLocalMappings(delRangeStart);

        // Parse hex strings into bytes
        byte startSrcByte = byte.Parse(startSrcHex, NumberStyles.HexNumber);
        byte endSrcByte = byte.Parse(endSrcHex, NumberStyles.HexNumber);

        // Convert to single-byte arrays
        byte[] srcRangeStart = new byte[] { startSrcByte };
        byte[] srcRangeEnd = new byte[] { endSrcByte };


        // Create new mapping in src and target shards - contacts
        Shard sourceShard1 = shardMap1.GetShards().FirstOrDefault(s => s.Location.Equals(sourceLocation));
        Shard targetShard1 = shardMap1.GetShards().FirstOrDefault(s => s.Location.Equals(targetLocation));

        if (targetShard1 == null)
        {
            targetShard1 = shardMap1.CreateShard(targetLocation);
            Console.WriteLine($"Created new shard: {targetDbName}");
        }

        var newSrcMapping1 = shardMap1.CreateRangeMapping(
            new Range<byte[]>(srcRangeStart, srcRangeEnd), sourceShard1);

        var newTarMapping1 = shardMap1.CreateRangeMapping(
            new Range<byte[]>(rangeStart, rangeEnd), targetShard1);

        //contact identifiers
        Shard sourceShard2 = shardMap2.GetShards().FirstOrDefault(s => s.Location.Equals(sourceLocation));
        Shard targetShard2 = shardMap2.GetShards().FirstOrDefault(s => s.Location.Equals(targetLocation));

        var newSrcMapping2 = shardMap2.CreateRangeMapping(
           new Range<byte[]>(srcRangeStart, srcRangeEnd), sourceShard2);

        var newTarMapping2 = shardMap2.CreateRangeMapping(
            new Range<byte[]>(rangeStart, rangeEnd), targetShard2);

        //device profiles
        Shard sourceShard3 = shardMap3.GetShards().FirstOrDefault(s => s.Location.Equals(sourceLocation));
        Shard targetShard3 = shardMap3.GetShards().FirstOrDefault(s => s.Location.Equals(targetLocation));

        var newSrcMapping3 = shardMap3.CreateRangeMapping(
           new Range<byte[]>(srcRangeStart, srcRangeEnd), sourceShard3);

        var newTarMapping3 = shardMap3.CreateRangeMapping(
            new Range<byte[]>(rangeStart, rangeEnd), targetShard3);

        Console.WriteLine("Migrating maps in selected range...");

        using (var conn = new SqlConnection($"Data Source={server};Initial Catalog={targetDbName};User ID={sqlAdminUser};Password={sqlAdminPwd};"))
        {
            conn.Open();
            using (var cmd = conn.CreateCommand())
            {

                string moveContactsSql = @"
                INSERT INTO [xdb_collection].[Contacts]
                SELECT * FROM OPENDATASOURCE('SQLNCLI', 'Data Source={0};User ID={1};Password={2}').[{3}].xdb_collection.Contacts
                WHERE Shardkey >= CAST(@start AS varbinary(1))
                  AND Shardkey <=  CAST(@end AS varbinary(1))";

                cmd.CommandText = string.Format(moveContactsSql, server, sqlAdminUser, sqlAdminPwd, sourceDbName);
                cmd.Parameters.AddWithValue("@start", rangeStart);
                cmd.Parameters.AddWithValue("@end", rangeEnd);
                int rows = cmd.ExecuteNonQuery();
                Console.WriteLine($"✓ Moved {rows} contacts to {targetDbName}");
            }


            using (var cmd = conn.CreateCommand())
            {
                string moveContactIdsSql = @"
                INSERT INTO [xdb_collection].[ContactIdentifiers]
                SELECT * FROM OPENDATASOURCE('SQLNCLI', 'Data Source={0};User ID={1};Password={2}').[{3}].xdb_collection.ContactIdentifiers
                WHERE Shardkey >= CAST(@start AS varbinary(1))
                  AND Shardkey <=  CAST(@end AS varbinary(1))";

                cmd.CommandText = string.Format(moveContactIdsSql, server, sqlAdminUser, sqlAdminPwd, sourceDbName);
                cmd.Parameters.AddWithValue("@start", rangeStart);
                cmd.Parameters.AddWithValue("@end", rangeEnd);
                int rows = cmd.ExecuteNonQuery();
                Console.WriteLine($"✓ Moved {rows} contacts to {targetDbName}");
            }

            using (var cmd = conn.CreateCommand())
            {
                string moveContactInteractionsSql = @"
                INSERT INTO [xdb_collection].[Interactions]
                SELECT * FROM OPENDATASOURCE('SQLNCLI', 'Data Source={0};User ID={1};Password={2}').[{3}].xdb_collection.Interactions
                WHERE Shardkey >= CAST(@start AS varbinary(1))
                  AND Shardkey <=  CAST(@end AS varbinary(1))";

                cmd.CommandText = string.Format(moveContactInteractionsSql, server, sqlAdminUser, sqlAdminPwd, sourceDbName);
                cmd.Parameters.AddWithValue("@start", rangeStart);
                cmd.Parameters.AddWithValue("@end", rangeEnd);
                int rows = cmd.ExecuteNonQuery();
                Console.WriteLine($"✓ Moved {rows} interactions to {targetDbName}");
            }

            using (var cmd = conn.CreateCommand())
            {
                string moveContactFacetsSql = @"
                INSERT INTO [xdb_collection].[ContactFacets]
                SELECT * FROM OPENDATASOURCE('SQLNCLI', 'Data Source={0};User ID={1};Password={2}').[{3}].xdb_collection.ContactFacets
                WHERE Shardkey >= CAST(@start AS varbinary(1))
                  AND Shardkey <=  CAST(@end AS varbinary(1))";

                cmd.CommandText = string.Format(moveContactFacetsSql, server, sqlAdminUser, sqlAdminPwd, sourceDbName);
                cmd.Parameters.AddWithValue("@start", rangeStart);
                cmd.Parameters.AddWithValue("@end", rangeEnd);
                int rows = cmd.ExecuteNonQuery();
                Console.WriteLine($"✓ Moved {rows} contact facets to {targetDbName}");
            }

            using (var cmd = conn.CreateCommand())
            {
                string moveContactFacetsSql = @"
                INSERT INTO [xdb_collection].[InteractionFacets]
                SELECT * FROM OPENDATASOURCE('SQLNCLI', 'Data Source={0};User ID={1};Password={2}').[{3}].xdb_collection.InteractionFacets
                WHERE Shardkey >= CAST(@start AS varbinary(1))
                  AND Shardkey <=  CAST(@end AS varbinary(1))";

                cmd.CommandText = string.Format(moveContactFacetsSql, server, sqlAdminUser, sqlAdminPwd, sourceDbName);
                cmd.Parameters.AddWithValue("@start", rangeStart);
                cmd.Parameters.AddWithValue("@end", rangeEnd);
                int rows = cmd.ExecuteNonQuery();
                Console.WriteLine($"✓ Moved {rows} contact facets to {targetDbName}");
            }

            using (var cmd = conn.CreateCommand())
            {
                string sql = @"
                INSERT INTO [xdb_collection].[DeviceProfiles]
                SELECT * FROM OPENDATASOURCE('SQLNCLI', 'Data Source={0};User ID={1};Password={2}').[{3}].xdb_collection.DeviceProfiles
                WHERE Shardkey >= CAST(@start AS varbinary(1))
                  AND Shardkey <=  CAST(@end AS varbinary(1))";

                cmd.CommandText = string.Format(sql, server, sqlAdminUser, sqlAdminPwd, sourceDbName);
                cmd.Parameters.AddWithValue("@start", rangeStart);
                cmd.Parameters.AddWithValue("@end", rangeEnd);
                int rows = cmd.ExecuteNonQuery();
                Console.WriteLine($"✓ Moved {rows} contact device profiles to {targetDbName}");
            }

            using (var cmd = conn.CreateCommand())
            {
                string sql = @"
                INSERT INTO [xdb_collection].[DeviceProfileFacets]
                SELECT * FROM OPENDATASOURCE('SQLNCLI', 'Data Source={0};User ID={1};Password={2}').[{3}].xdb_collection.DeviceProfileFacets
                WHERE Shardkey >= CAST(@start AS varbinary(1))
                  AND Shardkey <=  CAST(@end AS varbinary(1))";

                cmd.CommandText = string.Format(sql, server, sqlAdminUser, sqlAdminPwd, sourceDbName);
                cmd.Parameters.AddWithValue("@start", rangeStart);
                cmd.Parameters.AddWithValue("@end", rangeEnd);
                int rows = cmd.ExecuteNonQuery();
                Console.WriteLine($"✓ Moved {rows} contact device profile facets to {targetDbName}");
            }
        }

        Console.WriteLine("Starting Cleanup in source DB..");

        using (var conn = new SqlConnection($"Data Source={server};Initial Catalog={sourceDbName};User ID={sqlAdminUser};Password={sqlAdminPwd};"))
        {
            conn.Open();            
            using (var cmd = conn.CreateCommand())
            {

                string deleteContactIdsSql = @"           
                DELETE FROM OPENDATASOURCE('SQLNCLI', 'Data Source={0};User ID={1};Password={2}').[{3}].xdb_collection.ContactIdentifiers
                WHERE Shardkey >= CAST(@start AS varbinary(1))
                  AND Shardkey <=  CAST(@end AS varbinary(1))";

                cmd.CommandText = string.Format(deleteContactIdsSql, server, sqlAdminUser, sqlAdminPwd, sourceDbName);
                cmd.Parameters.AddWithValue("@start", rangeStart);
                cmd.Parameters.AddWithValue("@end", rangeEnd);
                int rows = cmd.ExecuteNonQuery();
                Console.WriteLine($"✓ Deleted {rows} contacts in {sourceDbName}");
            }     

        
            using (var cmd = conn.CreateCommand())
            {
                string deleteContactsSql = @"           
                DELETE FROM OPENDATASOURCE('SQLNCLI', 'Data Source={0};User ID={1};Password={2}').[{3}].xdb_collection.Contacts
                WHERE Shardkey >= CAST(@start AS varbinary(1))
                  AND Shardkey <=  CAST(@end AS varbinary(1))";

                cmd.CommandText = string.Format(deleteContactsSql,server, sqlAdminUser, sqlAdminPwd, sourceDbName);
                cmd.Parameters.AddWithValue("@start", rangeStart);
                cmd.Parameters.AddWithValue("@end", rangeEnd);
                int rows = cmd.ExecuteNonQuery();
                Console.WriteLine($"✓ Deleted {rows} contacts in {sourceDbName}");
            }

            using (var cmd = conn.CreateCommand())
            {
                string sql = @"           
                DELETE FROM OPENDATASOURCE('SQLNCLI', 'Data Source={0};User ID={1};Password={2}').[{3}].xdb_collection.ContactFacets
                WHERE Shardkey >= CAST(@start AS varbinary(1))
                  AND Shardkey <=  CAST(@end AS varbinary(1))";

                cmd.CommandText = string.Format(sql,  server, sqlAdminUser, sqlAdminPwd, sourceDbName);
                cmd.Parameters.AddWithValue("@start", rangeStart);
                cmd.Parameters.AddWithValue("@end", rangeEnd);
                int rows = cmd.ExecuteNonQuery();
                Console.WriteLine($"✓ Deleted {rows} contact facets in {sourceDbName}");
            }

            using (var cmd = conn.CreateCommand())
            {
                string sql = @"           
                DELETE FROM OPENDATASOURCE('SQLNCLI', 'Data Source={0};User ID={1};Password={2}').[{3}].xdb_collection.InteractionFacets
                WHERE Shardkey >= CAST(@start AS varbinary(1))
                  AND Shardkey <=  CAST(@end AS varbinary(1))";

                cmd.CommandText = string.Format(sql, server, sqlAdminUser, sqlAdminPwd, sourceDbName);
                cmd.Parameters.AddWithValue("@start", rangeStart);
                cmd.Parameters.AddWithValue("@end", rangeEnd);
                int rows = cmd.ExecuteNonQuery();
                Console.WriteLine($"✓ Deleted {rows} interaction facets in {sourceDbName}");
            }

            using (var cmd = conn.CreateCommand())
            {
                string sql = @"           
                DELETE FROM OPENDATASOURCE('SQLNCLI', 'Data Source={0};User ID={1};Password={2}').[{3}].xdb_collection.DeviceProfileFacets
                WHERE Shardkey >= CAST(@start AS varbinary(1))
                  AND Shardkey <=  CAST(@end AS varbinary(1))";

                cmd.CommandText = string.Format(sql,  server, sqlAdminUser, sqlAdminPwd, sourceDbName);
                cmd.Parameters.AddWithValue("@start", rangeStart);
                cmd.Parameters.AddWithValue("@end", rangeEnd);
                int rows = cmd.ExecuteNonQuery();
                Console.WriteLine($"✓ Deleted {rows} device profile facets in {sourceDbName}");
            }

            using (var cmd = conn.CreateCommand())
            {
                string sql = @"           
                DELETE FROM OPENDATASOURCE('SQLNCLI', 'Data Source={0};User ID={1};Password={2}').[{3}].xdb_collection.DeviceProfiles
                WHERE Shardkey >= CAST(@start AS varbinary(1))
                  AND Shardkey <=  CAST(@end AS varbinary(1))";

                cmd.CommandText = string.Format(sql,  server, sqlAdminUser, sqlAdminPwd, sourceDbName);
                cmd.Parameters.AddWithValue("@start", rangeStart);
                cmd.Parameters.AddWithValue("@end", rangeEnd);
                int rows = cmd.ExecuteNonQuery();
                Console.WriteLine($"✓ Deleted {rows} device profiles in {sourceDbName}");
            }

            using (var cmd = conn.CreateCommand())
            {
                string sql = @"           
                DELETE FROM OPENDATASOURCE('SQLNCLI', 'Data Source={0};User ID={1};Password={2}').[{3}].xdb_collection.Interactions
                WHERE Shardkey >= CAST(@start AS varbinary(1))
                  AND Shardkey <=  CAST(@end AS varbinary(1))";

                cmd.CommandText = string.Format(sql,  server, sqlAdminUser, sqlAdminPwd, sourceDbName);
                cmd.Parameters.AddWithValue("@start", rangeStart);
                cmd.Parameters.AddWithValue("@end", rangeEnd);
                int rows = cmd.ExecuteNonQuery();
                Console.WriteLine($"✓ Deleted {rows} interactions in {sourceDbName}");
            }
        }

        Console.WriteLine("Split complete.");
    }

    static void AddShardtoDefaultSetup(ShardMapManager smm)
    {
        // Step 1: List available shard maps
        var shardMaps = smm.GetShardMaps()
            .Where(m => m.MapType == ShardMapType.Range)
            .ToList();

        if (!shardMaps.Any())
        {
            Console.WriteLine("No shard maps found. You can create one now.");
        }
        else
        {
            Console.WriteLine("Available Shard Maps:");
            for (int i = 0; i < shardMaps.Count; i++)
            {
                Console.WriteLine($"{i + 1}. {shardMaps[i].Name}");
            }
        }

        Console.Write("Enter number to select a shard map or type a new name to create: ");
        var input = Console.ReadLine();

        RangeShardMap<byte[]> shardMap = null;

        string selectedName = null;

        if (int.TryParse(input, out int index) && index >= 1 && index <= shardMaps.Count)
        {
            selectedName = shardMaps[index - 1].Name;
            smm.TryGetRangeShardMap<byte[]>(selectedName, out shardMap);
        }
        else
        {
            selectedName = input.Trim();
            if (!smm.TryGetRangeShardMap<byte[]>(selectedName, out shardMap))
            {
                Console.WriteLine($"Creating shard map: {selectedName}");
                shardMap = smm.CreateRangeShardMap<byte[]>(selectedName);
            }
        }

        Console.WriteLine($"\nUsing Shard Map: {selectedName}");

        // Step 2: List existing mappings in the selected shard map
        var mappings = shardMap.GetMappings();
        if (mappings.Any())
        {
            Console.WriteLine("\nExisting Mappings:");
            foreach (var mapping in mappings)
            {
                Console.WriteLine($"- Range [{ByteToHex(mapping.Value.Low)} - {ByteToHex(mapping.Value.High)}) -> {mapping.Shard.Location.Database}");
            }
        }
        else
        {
            Console.WriteLine("No mappings registered yet.");
        }

        // Step 3: Distribute new shards evenly
        Console.Write("\nEnter number of additional shards to map: ");
        int additionalShards = int.Parse(Console.ReadLine());

        Console.Write("Enter shard name prefix (e.g., sc104k_Xdb.Collection.Shard): ");
        string shardPrefix = Console.ReadLine();

        Console.Write("Shard Deployment option (1/2): ");
        string depOption = Console.ReadLine();

        int existingShardCount = shardMap.GetShards().Count();

        List<Shard> shards = shardMap.GetShards()
        .OrderBy(s =>
        {
            var dbName = s.Location.Database;
            var digits = new string(dbName.Reverse().TakeWhile(char.IsDigit).Reverse().ToArray());
            return int.TryParse(digits, out int num) ? num : int.MaxValue;
        })
        .ToList();

        using (var scope = new TransactionScope(TransactionScopeOption.Required))
        {

            for (int i = 0; i < additionalShards; i++)
            {
                string dbName = $"{shardPrefix}{existingShardCount + i}";
                switch (depOption)
                {
                    case "1":
                        //create shard db - start                    
                        CreateDB(dbName);
                        //create shard db - end

                        //register shard - start
                        var builder = new System.Data.SqlClient.SqlConnectionStringBuilder
                        {
                            DataSource = server,
                            InitialCatalog = dbName,
                            UserID = dbUser,
                            Password = dbPwd
                        };

                        var location = new ShardLocation(builder.DataSource, builder.InitialCatalog);
                        var shard = shardMap.CreateShard(location);
                        shards.Add(shard);
                        Console.WriteLine($"Registered shard: {dbName}");
                        //register shard - end
                        break;
                    case "2":
                        //for contact - related db tables - start
                        SetupDBwithSchema(dbName);
                        //end
                        break;
                    default:
                        break;
                }
            }

            DeleteGlobalMappings(shardMap);

            var mappingsToDelete = shardMap.GetMappings().ToList();
            DeleteLocalMappings(mappingsToDelete);

            shards.Clear();
            shards = shardMap.GetShards()
            .OrderBy(s =>
            {
                var dbName = s.Location.Database;
                var digits = new string(dbName.Reverse().TakeWhile(char.IsDigit).Reverse().ToArray());
                return int.TryParse(digits, out int num) ? num : int.MaxValue;
            })
            .ToList(); ;
            int totalShards = shardMap.GetShards().Count();

            // Full byte range: 0x00 to 0xFF + 1
            int totalRange = 256;
            int rangeSize = totalRange / totalShards;
            // Recreate evenly distributed mappings
            for (int i = 0; i < totalShards; i++)
            {
                byte start = (byte)(i * rangeSize);
                byte end = (i == totalShards - 1) ? (byte)0xFF : (byte)((i + 1) * rangeSize);

                byte[] minBytes = new byte[] { start };
                byte[] maxBytes = (i == totalShards - 1) ? null : new byte[] { end }; // null = max

                var range = new Range<byte[]>(minBytes, maxBytes);
                shardMap.CreateRangeMapping(range, shards[i]);

                Console.WriteLine($"Mapped range [{ByteToHex(minBytes)} - {ByteToHex(maxBytes)}) to {shards[i].Location.Database}");
            }
            scope.Complete(); // Commits the distributed transaction
        }
    }

    static void DeleteSrcLocalMappings(byte[] minVal = null)
    {
            try
            {
                // Delete from local shard mapping
                using (var conn = new System.Data.SqlClient.SqlConnection(
                    $"Data Source={server};Initial Catalog={sourceDbName};User ID={sqlAdminUser};Password={sqlAdminPwd};"))
                {
                    conn.Open();
                    using (var cmd = conn.CreateCommand())
                    {
                        if (minVal != null)
                        {
                            cmd.CommandText = @"
                                DELETE FROM __ShardManagement.ShardMappingsLocal 
                                WHERE minvalue=@minVal
                            ";
                            cmd.Parameters.AddWithValue("@minVal", minVal);
                        }
                        else
                        {
                            cmd.CommandText = @"
                            DELETE FROM __ShardManagement.ShardMappingsLocal
                           ";
                        }
                        cmd.ExecuteNonQuery();
                    }
                }

                Console.WriteLine("✓ Deleted from local mapping tables");
            }

            catch (Exception ex)
            {
                Console.WriteLine($"✗ Failed to delete mapping: {ex.Message}");
            }
    }

    static void DeleteExistingGlobalMappings(byte[] minVal = null)
    {
        // Clear existing mappings in global shard map manager
        Console.WriteLine($"Deleting global mappings");

        using (var conn = new System.Data.SqlClient.SqlConnection(smmConnString))
        {
            conn.Open();
            using (var cmd = conn.CreateCommand())
            {
                if (minVal != null)
                {
                    cmd.CommandText = @"
                        DELETE FROM __ShardManagement.ShardMappingsGlobal 
                        WHERE minvalue=@minVal";
                    cmd.Parameters.AddWithValue("@minVal", minVal);
                }
                else
                {
                    cmd.CommandText = @"
                        DELETE FROM __ShardManagement.ShardMappingsGlobal";
                }

                cmd.ExecuteNonQuery();
                Console.WriteLine($"Deleted global mappings");
            }
        }
    }



    static void DeleteLocalMappings(List<RangeMapping<byte[]>> mappingsToDelete, byte[] minVal = null)
    {
        foreach (var mapping in mappingsToDelete)
        {
            try
            {
                // Delete from local shard mapping
                using (var conn = new System.Data.SqlClient.SqlConnection(
                    $"Data Source={mapping.Shard.Location.DataSource};Initial Catalog={mapping.Shard.Location.Database};User ID={sqlAdminUser};Password={sqlAdminPwd};"))
                {
                    conn.Open();
                    using (var cmd = conn.CreateCommand())
                    {
                        if (minVal != null)
                        {
                             cmd.CommandText = @"
                                DELETE FROM __ShardManagement.ShardMappingsLocal 
                                WHERE minvalue=@minVal
                            )";
                            cmd.Parameters.AddWithValue("@minVal", minVal);
                        }
                        else
                        {
                            cmd.CommandText = @"
                            DELETE FROM __ShardManagement.ShardMappingsLocal
                           ";
                        }
                        cmd.ExecuteNonQuery();
                    }
                }

                Console.WriteLine("✓ Deleted from global and local mapping tables");
            }

            catch (Exception ex)
            {
                Console.WriteLine($"✗ Failed to delete mapping: {ex.Message}");
            }
        }
    }

    static void DeleteGlobalMappings(RangeShardMap<byte[]> shardMap, byte[] minVal=null)
    {
        // Clear existing mappings in global shard map manager
        Console.WriteLine($"Deleting global mappings");

        using (var conn = new System.Data.SqlClient.SqlConnection(smmConnString))
        {
            conn.Open();
            using (var cmd = conn.CreateCommand())
            {
                if (minVal != null)
                {
                        cmd.CommandText = @"
                        DELETE FROM __ShardManagement.ShardMappingsGlobal 
                        WHERE minvalue=@minVal";
                        cmd.Parameters.AddWithValue("@minVal", minVal);
                }
                else
                {
                    cmd.CommandText = @"
                        DELETE FROM __ShardManagement.ShardMappingsGlobal";
                }
                    
                cmd.ExecuteNonQuery();
                Console.WriteLine($"Deleted global mappings");
            }
        }
    }

    static void CreateDB(string dbName)
    {
        string shardConnectionString = $"Data Source={server};Initial Catalog={dbName};User ID={sqlAdminUser};Password={sqlAdminPwd};";

        // Ensure physical shard DB exists before registering
        using (var conn = new System.Data.SqlClient.SqlConnection($"Data Source={server};Initial Catalog=master;User ID={sqlAdminUser};Password={sqlAdminPwd};"))
        {
            conn.Open();
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = $"IF DB_ID('{dbName}') IS NULL CREATE DATABASE [{dbName}];";
                cmd.ExecuteNonQuery();
            }
        }
    }

    static void SetupDBwithSchema(string dbName)
    {
        string dacpacPath = ConfigurationManager.AppSettings["xdbDacpacPath"];

        Console.WriteLine($"Deploying XDB schema to {dbName}...");

        var registerCommand = $@"/operation addShard " +
                            $@"/connectionstring ""{smmConnString}"" " +
                            $@"/shardMapManagerDatabaseName  ""{shardMapManagerDBName}"" " +
                            $@"/shardConnectionString ""Data Source={server};User ID={dbUser};Password={dbPwd}"" " +
                            $@"/shardnameprefix ""{shardPrefix}"" " +
                            $@"/dacpac ""{dacpacPath}""";

        if (!System.IO.File.Exists(toolPath))
        {
            Console.WriteLine("ERROR: DACPAC deployment tool not found at: " + toolPath);
            return;
        }

        int exitCode = DeploySchemaAsync(toolPath, registerCommand).GetAwaiter().GetResult();
        if (exitCode != 0)
        {
            Console.WriteLine($"✗ Schema registration failed for {dbName}");
            return;
        }
        //for contact - related db tables - end

        //create the collection user else, contacts won't be inserted - start
        using (var conn = new SqlConnection($"Data Source={server};Initial Catalog={dbName};User ID={sqlAdminUser};Password={sqlAdminPwd};"))
        {
            conn.Open();
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = $@"IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = '{dbUser}')
                                        BEGIN
                                            CREATE USER [{dbUser}];
                                            EXEC sp_addrolemember 'db_datareader', '{dbUser}';
                                            EXEC sp_addrolemember 'db_datawriter', '{dbUser}';
                                            GRANT EXECUTE TO [{dbUser}];
                                        END";
                cmd.ExecuteNonQuery();
            }
        }
    }

    static async Task<int> DeploySchemaAsync(string toolPath, string arguments)
    {
        var process = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = toolPath,
                Arguments = arguments,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            },
            EnableRaisingEvents = true
        };

        var outputBuilder = new StringBuilder();
        var errorBuilder = new StringBuilder();

        process.OutputDataReceived += (s, e) => outputBuilder.AppendLine(e.Data);
        process.ErrorDataReceived += (s, e) => errorBuilder.AppendLine(e.Data);

        var tcs = new TaskCompletionSource<int>();

        process.Exited += (s, e) => tcs.SetResult(process.ExitCode);

        process.Start();
        process.BeginOutputReadLine();
        process.BeginErrorReadLine();

        int exitCode = await tcs.Task;

        Console.WriteLine(outputBuilder.ToString());

        if (exitCode != 0)
            Console.WriteLine($"✗ Error: {errorBuilder.ToString()}");

        return exitCode;
    }

    static int ByteArrayCompare(byte[] a, byte[] b)
    {
        if (a == null && b == null) return 0;
        if (a == null) return 1; // null = max
        if (b == null) return -1;

        for (int i = 0; i < Math.Min(a.Length, b.Length); i++)
        {
            int cmp = a[i].CompareTo(b[i]);
            if (cmp != 0) return cmp;
        }
        return a.Length.CompareTo(b.Length);
    }

    static bool RangesOverlap(byte[] aMin, byte[] aMax, byte[] bMin, byte[] bMax)
    {
        return ByteArrayCompare(aMin, bMax) < 0 && ByteArrayCompare(bMin, aMax) < 0;
    }

    static string ByteToHex(byte[] bytes) => bytes == null ? "MAX" : BitConverter.ToString(bytes).Replace("-", "");

    static byte[] HexToByteArray(string hex)
    {
        return Enumerable.Range(0, hex.Length / 2)
                         .Select(x => Convert.ToByte(hex.Substring(x * 2, 2), 16))
                         .ToArray();
    }
}
