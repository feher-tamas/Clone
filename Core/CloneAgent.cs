using NetMQ;
using NetMQ.Sockets;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;

namespace Core
{
    public class CloneAgent
    {
        private readonly string[] _serverEndpoints; // Pl: "localhost:5556", "localhost:5566"
        private int _currentHostIndex = 0;
        private readonly ConcurrentDictionary<string, KvMsg> _cache;
        private long _lastSyncSequence = -1;
        private DateTime _lastActivity;
        private readonly TimeSpan _timeout = TimeSpan.FromSeconds(3);
        private PublisherSocket _collectorClient;

        public CloneAgent(string[] endpoints, ConcurrentDictionary<string, KvMsg> cache)
        {
            _serverEndpoints = endpoints;
            _cache = cache;
        }

        // Beküldés a szervernek (Collector port: P+2)
        public void Set(string key, byte[] value)
        {
            if (_collectorClient == null) return;

            var kv = new KvMsg { Key = key, UUID = Guid.NewGuid(), Body = value };
            var msg = new NetMQMessage();
            msg.Append("KVSET");
            kv.AppendToMessage(msg);

            // Naplózás a teszteléshez
            Console.WriteLine($"[Agent] Küldés a szervernek: {key}");
            _collectorClient.SendMultipartMessage(msg);
        }

        public void Run(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                // Végpont elemzése (IP:Port)
                string endpoint = _serverEndpoints[_currentHostIndex];
                string host = endpoint.Split(':')[0];
                int p = int.Parse(endpoint.Split(':')[1]);

                using (var snapshot = new DealerSocket($">tcp://{host}:{p}"))
                using (var subscriber = new SubscriberSocket($">tcp://{host}:{p + 1}"))
                using (_collectorClient = new PublisherSocket($">tcp://{host}:{p + 2}"))
                using (var poller = new NetMQPoller { subscriber })
                {
                    subscriber.SubscribeToAnyTopic();
                    _lastActivity = DateTime.Now;

                    // 1. Snapshot kérés
                    var req = new NetMQMessage();
                    req.Append("ICANHAZ?");
                    req.Append("");
                    snapshot.SendMultipartMessage(req);

                    // 2. Szinkronizáció
                    bool syncDone = false;
                    while (!syncDone && !token.IsCancellationRequested)
                    {
                        NetMQMessage msg = null;
                        if (snapshot.TryReceiveMultipartMessage(TimeSpan.FromSeconds(2), ref msg))
                        {
                            _lastActivity = DateTime.Now;
                            if (msg[0].ConvertToString() == "KVSYNC")
                            {
                                var kv = KvMsg.FromFrames(msg, 1);
                                _cache[kv.Key] = kv;
                            }
                            else if (msg[0].ConvertToString() == "KTHXBAI")
                            {
                                _lastSyncSequence = BitConverter.ToInt64(msg[1].ToByteArray(), 0);
                                syncDone = true;
                            }
                        }
                        else break; // Timeout -> Failover
                    }

                    if (!syncDone) { SwitchServer(); continue; }

                    // 3. Eseménykezelés
                    subscriber.ReceiveReady += (s, e) => {
                        var msg = subscriber.ReceiveMultipartMessage();
                        _lastActivity = DateTime.Now;
                        if (msg[0].ConvertToString() == "KVPUB")
                        {
                            var kv = KvMsg.FromFrames(msg, 1);
                            if (kv.Sequence > _lastSyncSequence)
                            {
                                _lastSyncSequence = kv.Sequence;
                                _cache[kv.Key] = kv;
                            }
                        }
                    };

                    var timer = new NetMQTimer(TimeSpan.FromMilliseconds(500));
                    timer.Elapsed += (s, e) => {
                        if (DateTime.Now - _lastActivity > _timeout) poller.Stop();
                    };
                    poller.Add(timer);
                    poller.Run();
                }
                _collectorClient = null;
                SwitchServer();
            }
        }

        private void SwitchServer() => _currentHostIndex = (_currentHostIndex + 1) % _serverEndpoints.Length;
    }
}
