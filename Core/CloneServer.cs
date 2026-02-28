using NetMQ;
using NetMQ.Sockets;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;

namespace Core
{
    public class CloneServer
    {
        private readonly ConcurrentDictionary<string, KvMsg> _kvStore = new();
        private long _sequence = 0;

        public void Start(int port, string peerHost = null)
        {
            bool isBackup = !string.IsNullOrEmpty(peerHost);

            using (var publisher = new PublisherSocket($"@tcp://*:{port + 1}"))   // P+1: KVPUB
            using (var router = new RouterSocket($"@tcp://*:{port}"))            // P: SNAPSHOT
            using (var collector = new SubscriberSocket($"@tcp://*:{port + 2}")) // P+2: KVSET
            using (var peerSub = new SubscriberSocket())
            using (var poller = new NetMQPoller { publisher, router, collector })
            {
                collector.SubscribeToAnyTopic(); // Fontos: feliratkozás a kliens üzenetekre

                Console.WriteLine("========================================");
                Console.WriteLine($"[SZERVER] Indítás a porton: {port}");
                Console.WriteLine($"[MÓD] {(isBackup ? "BACKUP (Passzív)" : "PRIMARY (Aktív)")}");
                Console.WriteLine("========================================");

                // --- 1. SZINKRONIZÁCIÓ (Ha Backup módban van) ---
                if (isBackup)
                {
                    peerSub.Connect($"tcp://{peerHost}:{port + 1}");
                    peerSub.SubscribeToAnyTopic();
                    peerSub.ReceiveReady += (s, e) =>
                    {
                        var msg = peerSub.ReceiveMultipartMessage();
                        if (msg[0].ConvertToString() == "KVPUB")
                        {
                            var kv = KvMsg.FromFrames(msg, 1);
                            _kvStore[kv.Key] = kv;
                            // Szekvencia szinkronizálása: ne induljon újra 0-ról
                            Interlocked.Exchange(ref _sequence, Math.Max(Interlocked.Read(ref _sequence), kv.Sequence));
                            Console.WriteLine($"[BACKUP SYNC] Adat érkezett a Primary-től: {kv.Key} (Seq: {kv.Sequence})");
                        }
                    };
                    poller.Add(peerSub);
                }

                // --- 2. PILLANATKÉP (Snapshot) KEZELÉSE ---
                router.ReceiveReady += (s, e) =>
                {
                    var request = router.ReceiveMultipartMessage();
                    var identity = request[0];
                    if (request[1].ConvertToString() == "ICANHAZ?")
                    {
                        Console.WriteLine($"[SNAPSHOT] Kérés érkezett: {identity.ConvertToString()}");
                        foreach (var item in _kvStore.Values)
                        {
                            var response = new NetMQMessage();
                            response.Append(identity);
                            response.Append("KVSYNC");
                            item.AppendToMessage(response);
                            router.SendMultipartMessage(response);
                        }

                        var endMsg = new NetMQMessage();
                        endMsg.Append(identity);
                        endMsg.Append("KTHXBAI");
                        endMsg.Append(BitConverter.GetBytes(Interlocked.Read(ref _sequence)));
                        router.SendMultipartMessage(endMsg);
                        Console.WriteLine($"[SNAPSHOT] Pillanatkép lezárva. Utolsó Seq: {Interlocked.Read(ref _sequence)}");
                    }
                };

                // --- 3. GYŰJTŐ (Collector) KEZELÉSE ---
                collector.ReceiveReady += (s, e) =>
                {
                    var msg = collector.ReceiveMultipartMessage();
                    if (msg[0].ConvertToString() == "KVSET")
                    {
                        var kv = KvMsg.FromFrames(msg, 1);
                        kv.Sequence = Interlocked.Increment(ref _sequence); // Új szekvencia osztása
                        _kvStore[kv.Key] = kv;

                        Console.WriteLine($"[COLLECTOR] Új KVSET: {kv.Key} = Seq:{kv.Sequence} (UUID: {kv.UUID})");

                        var pubMsg = new NetMQMessage();
                        pubMsg.Append("KVPUB");
                        kv.AppendToMessage(pubMsg);
                        publisher.SendMultipartMessage(pubMsg); // Republish mindenki számára
                    }
                };

                // --- 4. HEARTBEAT (HUGZ) ---
                var heartbeatTimer = new NetMQTimer(TimeSpan.FromSeconds(1));
                heartbeatTimer.Elapsed += (s, e) =>
                {
                    var hugz = new NetMQMessage();
                    hugz.Append("HUGZ");
                    hugz.Append(BitConverter.GetBytes(0L));
                    hugz.Append(Guid.Empty.ToByteArray());
                    hugz.Append(Array.Empty<byte>());
                    publisher.SendMultipartMessage(hugz);
                };
                poller.Add(heartbeatTimer);

                poller.Run();
            }
        }

        public void UpdateValue(string key, byte[] data)
        {
            var kv = new KvMsg
            {
                Key = key,
                Sequence = Interlocked.Increment(ref _sequence),
                UUID = Guid.NewGuid(),
                Body = data
            };
            _kvStore[key] = kv;
        }
    }
}
