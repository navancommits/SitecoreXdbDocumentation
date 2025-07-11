using Azure;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;
using System;
using System.Data.SqlClient;
using System.Linq;
using System.Numerics;

namespace Sitecore.Documentation
{
    class Program
    {
        static void Main(string[] args)
        {
            //var min = new Guid("80000000-0000-0000-0000-000000000000").ToByteArray();
            //var max = new Guid("FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF").ToByteArray();
            //var min = new Guid("0x80").ToByteArray();
            //var max = new Guid("0xBF").ToByteArray();

            //var testGuid = new Guid("A678A7B8-ECE4-005F-0000-0751B2247B9D");
            //var testKey = testGuid.ToByteArray();

            //if (IsInRange(testKey, min, max))
            //{
            //    Console.WriteLine($"✅ GUID {testGuid} falls within range.");
            //}
            //else
            //{
            //    Console.WriteLine($"❌ GUID {testGuid} is outside of range.");
            //}

            // Simulated 1-byte min/max from backend
            //byte[] min = new byte[] { 0x80 };
            //byte[] max = new byte[] { 0xFF }; // exclusive

            //Guid testGuid = Guid.NewGuid();

            //if (IsGuidInRange(testGuid, min, max))
            //{
            //    Console.WriteLine($"✅ GUID {testGuid} falls in range.");
            //}
            //else
            //{
            //    Console.WriteLine($"❌ GUID {testGuid} is outside the range.");
            //}

            byte[] min = new byte[] { 0x80 };
            byte[] max = new byte[] { 0xFF };
            Guid testGuid = Guid.NewGuid();

            if (IsGuidInRange(testGuid, min, max))
            {
                Console.WriteLine($"✅ GUID {testGuid} is in range.");
            }
            else
            {
                Console.WriteLine($"❌ GUID {testGuid} is outside the range.");
            }

        }

        //static bool IsGuidInRange(Guid guid, byte[] minInclusive, byte[] maxExclusive)
        //{
        //    byte[] guidBytes = guid.ToByteArray();
        //    byte[] paddedMin = PadToSameLength(minInclusive, guidBytes.Length);
        //    byte[] paddedMax = PadToSameLength(maxExclusive, guidBytes.Length);

        //    return CompareBytes(guidBytes, paddedMin) >= 0 &&
        //           CompareBytes(guidBytes, paddedMax) < 0;
        //}

        static bool IsGuidInRange(Guid guid, byte[] minInclusive, byte[] maxExclusive)
        {
            byte[] guidBytes = guid.ToByteArray();

            return CompareBytes(guidBytes, minInclusive) >= 0 &&
                   CompareBytes(guidBytes, maxExclusive) < 0;
        }

        static byte[] PadToSameLength(byte[] source, int length)
        {
            if (source.Length == length)
                return source;

            var padded = new byte[length];
            Array.Copy(source, padded, Math.Min(source.Length, length));
            return padded;
        }

        static int CompareBytes(byte[] a, byte[] b)
        {
            int minLength = Math.Min(a.Length, b.Length);

            for (int i = 0; i < minLength; i++)
            {
                int diff = a[i].CompareTo(b[i]);
                if (diff != 0)
                    return diff;
            }

            // If all compared bytes match, shorter array is considered less
            return a.Length.CompareTo(b.Length);
        }

        //static int CompareBytes(byte[] a, byte[] b)
        //{
        //    for (int i = 0; i < Math.Min(a.Length, b.Length); i++)
        //    {
        //        int diff = a[i].CompareTo(b[i]);
        //        if (diff != 0)
        //            return diff;
        //    }
        //    return a.Length.CompareTo(b.Length); // fallback on length
        //}


        //static bool IsInRange(byte[] key, byte[] min, byte[] max)
        //{
        //    return CompareBytes(key, min) >= 0 && CompareBytes(key, max) < 0;
        //}

        //static int CompareBytes(byte[] a, byte[] b)
        //{
        //    int len = Math.Max(a.Length, b.Length);
        //    byte[] aPadded = new byte[len];
        //    byte[] bPadded = new byte[len];
        //    Array.Copy(a, 0, aPadded, 0, a.Length);
        //    Array.Copy(b, 0, bPadded, 0, b.Length);

        //    for (int i = 0; i < len; i++)
        //    {
        //        int diff = aPadded[i].CompareTo(bPadded[i]);
        //        if (diff != 0)
        //            return diff;
        //    }
        //    return 0;
        //}
    }
}