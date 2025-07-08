using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

class Program
{
    static void Main()
    {
        string smmConnString = ConfigurationManager.AppSettings["smmConnString"];
        string dbPwd = ConfigurationManager.AppSettings["DBPassword"];
        string dbUser = ConfigurationManager.AppSettings["DBUser"];
        string sqlAdminPwd = ConfigurationManager.AppSettings["SqlAdminPassword"];
        string sqlAdminUser = ConfigurationManager.AppSettings["SqlAdminUser"];
        string shardMapManagerDBName = ConfigurationManager.AppSettings["smmDbName"];
        string toolPath = ConfigurationManager.AppSettings["shardDeploymentToolPath"];

        ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(smmConnString, ShardMapManagerLoadPolicy.Lazy);

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

        Console.Write("Enter shard server name: ");
        string server = Console.ReadLine();

        Console.Write("Shard Deployment option (1/2): ");
        string depOption = Console.ReadLine();

        int existingShardCount = shardMap.GetShards().Count();

        List<Shard> shards = shardMap.GetShards()
        .OrderBy(s =>
        {            var dbName = s.Location.Database;
            var digits = new string(dbName.Reverse().TakeWhile(char.IsDigit).Reverse().ToArray());
            return int.TryParse(digits, out int num) ? num : int.MaxValue;
        })
        .ToList();

        for (int i = 0; i < additionalShards; i++)
        {
            string dbName = $"{shardPrefix}{existingShardCount + i}";
            switch (depOption)
            {
                case "1":
                    //create shard db - start                    
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
                    //end
                    break;
                default:
                    break;
            }            
        }

        // Clear existing mappings in both global and local shard map manager
        var mappingsToDelete = shardMap.GetMappings().ToList();
            
        Console.WriteLine($"Deleting global mappings");

        // Delete from global shard map
        using (var conn = new System.Data.SqlClient.SqlConnection(smmConnString))
        {
            conn.Open();
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = @"
                DELETE FROM __ShardManagement.ShardMappingsGlobal
                WHERE ShardMapId IN (
                    SELECT ShardMapId 
                    FROM __ShardManagement.ShardMapsGlobal
                    WHERE Name = 'ContactIdShardMap'
                )";
                //cmd.Parameters.AddWithValue("@server", mapping.Shard.Location.DataSource);
                //cmd.Parameters.AddWithValue("@database", mapping.Shard.Location.Database);
                cmd.ExecuteNonQuery();
                Console.WriteLine($"Deleted global mappings");
            }
        }

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
                        cmd.CommandText = @"
                            DELETE FROM __ShardManagement.ShardMappingsLocal
                            WHERE ShardMapId IN (
                                SELECT ShardMapId 
                                FROM __ShardManagement.ShardMapsLocal
                                WHERE Name = 'ContactIdShardMap'
                            )";
                        //cmd.Parameters.AddWithValue("@server", mapping.Shard.Location.DataSource);
                        //cmd.Parameters.AddWithValue("@database", mapping.Shard.Location.Database);
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
}
