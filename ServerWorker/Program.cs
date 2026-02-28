using Core;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;

// Használat: 
// Primary: dotnet run -- 5556
// Backup:  dotnet run -- 5566 localhost

var builder = Host.CreateApplicationBuilder(args);

// Port és Peer kinyerése az argumentumokból
int port = args.Length > 0 ? int.Parse(args[0]) : 5556;
string? peer = args.Length > 1 ? args[1] : null;

builder.Services.AddHostedService(sp => new CloneServerWorker(port, peer));

var host = builder.Build();
host.Run();

// A Worker osztály, ami meghívja a CloneServer-t
public class CloneServerWorker : BackgroundService
{
    private readonly int _port;
    private readonly string? _peer;

    public CloneServerWorker(int port, string? peer)
    {
        _port = port;
        _peer = peer;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var server = new CloneServer();

        // Inicializálunk egy alap értéket, hogy lássuk melyik szerver az aktív
        string role = _peer == null ? "Primary" : "Backup";
        server.UpdateValue("server.info", System.Text.Encoding.UTF8.GetBytes($"Role: {role}, Port: {_port}"));

        await Task.Run(() => server.Start(_port, _peer), stoppingToken);
    }
}