using System;
using System.IO;
using System.Diagnostics;
using System.Text;

/*
    To create a csharp console project, do the following
        1) In terminal, navigate to the folder where you want the program
        2) run in terminal - dotnet new console -n ProjectName
        3) Open vs code
        4) edit the Program.cs file that was created with what you want
        5) in terminal to debug/build/run do - dotnet run
*/

class Program
{
    static void Main()
    {

        int buffer_size = 1024 * 1024 * 10;
        int tot_rows = 1000000000;
        int batch_size = 10000;

        string homePath = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        string f_path = homePath+"/test_dummy_data/write_benchmark/csharp_generated.csv";

        Stopwatch stopwatch = new Stopwatch();
        stopwatch.Start();

        using (FileStream fs = new FileStream(f_path, FileMode.Create, FileAccess.Write, FileShare.None))
        using (StreamWriter writer = new StreamWriter(fs, Encoding.UTF8, bufferSize: buffer_size)) 
        {

            // Convert integers to bytes and write them to the file
            for (int i=1;i <= tot_rows;i ++)
            {
                writer.WriteLine(i);

                if(i % batch_size == 0) {
                    writer.Flush();
                }

            }

            
        }

        stopwatch.Stop();

        TimeSpan elapsedTime = stopwatch.Elapsed;
        double elapsedSeconds = elapsedTime.TotalSeconds;

        Console.WriteLine($"Successfully wrote {tot_rows.ToString("N0")} integers. Elapsed time: {elapsedSeconds} seconds");

    }
}


