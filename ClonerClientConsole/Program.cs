using Core;
using System.Collections.Concurrent;

internal class Program
{
    private static void Main(string[] args)
    {
        var cache = new ConcurrentDictionary<string, KvMsg>();
        var cts = new CancellationTokenSource();

        // Megadjuk mindkét localhost-os szerver portját
        string[] serverHosts = { "localhost:5556", "localhost:5566" };
        // Megjegyzés: A CloneAgent.cs-t módosítani kell, hogy a portot a host stringből vegye, 
        // vagy tartsuk meg az eredeti port kiosztást, ha csak az IP különbözik.

        var agent = new CloneAgent(serverHosts, cache);
        // Egyszerűség kedvéért a korábbi kódunk fix porteltolást (P, P+1, P+2) használt. 
        // Teszteléshez futtassunk két szervert két különböző IP-n vagy módosított portkezeléssel.

        Task.Run(() => agent.Run(cts.Token));

        Console.WriteLine("Teszt indítása. Figyeljük a cache változásait...");

        while (true)
        {
            Console.Clear();
            Console.WriteLine("--- Aktuális Állapot (Cache) ---");
            foreach (var item in cache)
            {
                string val = System.Text.Encoding.UTF8.GetString(item.Value.Body);
                Console.WriteLine($"Kulcs: {item.Key} | Érték: {val} | Seq: {item.Value.Sequence}");
            }

            Console.WriteLine("\nNyomj 'S'-t egy új érték beküldéséhez (Collector teszt), vagy 'Q' a kilépéshez.");
            if (Console.KeyAvailable)
            {
                var key = Console.ReadKey(true).Key;
                if (key == ConsoleKey.S)
                {
                    string testVal = $"Update-{DateTime.Now.ToShortTimeString()}";
                    agent.Set("user.action", System.Text.Encoding.UTF8.GetBytes(testVal));
                    Console.WriteLine($"Beküldve: user.action = {testVal}");
                }
                else if (key == ConsoleKey.Q) break;
            }
            Thread.Sleep(100);
        }
    }
}