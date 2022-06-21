using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;

namespace GoAl
{
    class Program
    {
        public static int LineCounter = 1000000;
        public static int SkipLine = 0;
        public static int Threads = 1;

        public static string FilePath = @"C:\Users\mert.erdinc\Desktop\Alict\datasets\dataset_2\small_lineitem.tbl";

        static void Main(string[] args)
        {
            int LastNumberOfFiles = 0;

            ParallelProcess(LastNumberOfFiles,FilePath);

        }

        public static List<string> ReadData()
        {
            List<string> lists = new List<string>();
            try
            {
                lists = File.ReadLines(FilePath).Skip(SkipLine).Take(LineCounter).ToList();
                SkipLine += LineCounter;
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }

            Threads++;
            return lists;
        }

        public static bool ParallelProcess(int lastNumberOfFiles, string filePath)
        {
            var newFilePath = $"C:\\Users\\mert.erdinc\\Desktop\\Alict\\datasets\\dataset_2\\small_lineitem{lastNumberOfFiles}.tbl";
            Thread.Sleep(500);
            using (StreamReader sr = new StreamReader(filePath))
            {
                while (File.ReadLines(FilePath).Skip(SkipLine).Take(1).Count() > 0)
                {
                    var lines = ReadData();
                    newFilePath = $"C:\\Users\\mert.erdinc\\Desktop\\Alict\\datasets\\dataset_2\\small_lineitem{lastNumberOfFiles}.tbl";
                    Thread thread = new Thread(() =>
                    {
                        ParallelProcess((Guid.NewGuid().GetHashCode()), filePath);
                    });
                    File.WriteAllLines(newFilePath, lines);
                    thread.Start();
                    thread.Join();
                    
                }
            }
            return true;
        }
    }
}
