using NetMQ;

namespace Core
{
    public class KvMsg
    {
        public string Key { get; set; }
        public long Sequence { get; set; }
        public Guid UUID { get; set; }
        public byte[] Body { get; set; }

        // Protokoll szerinti keretek összeállítása (RFC 12 alapú)
        public NetMQMessage ToNetMQMessage()
        {
            var msg = new NetMQMessage();
            msg.Append(Key);
            msg.Append(BitConverter.GetBytes(Sequence)); // Network byte order-re figyelni kell élesben
            msg.Append(UUID.ToByteArray());
            msg.Append(Body ?? Array.Empty<byte>());
            return msg;
        }

        public static KvMsg FromNetMQMessage(NetMQMessage msg)
        {
            return new KvMsg
            {
                Key = msg[0].ConvertToString(),
                Sequence = BitConverter.ToInt64(msg[1].ToByteArray(), 0),
                UUID = new Guid(msg[2].ToByteArray()),
                Body = msg[3].ToByteArray()
            };
        }
        public void AppendToMessage(NetMQMessage msg)
        {
            msg.Append(Key);
            msg.Append(BitConverter.GetBytes(Sequence));
            msg.Append(UUID.ToByteArray());
            msg.Append(Body ?? Array.Empty<byte>());
        }
        /// <summary>
        /// Segédmetódus a kliensnek: statikus gyár a KvMsg példányosításához
        /// a hálózaton érkezett üzenet kereteiből.
        /// </summary>
        public static KvMsg FromFrames(NetMQMessage msg, int startIndex)
        {
            return new KvMsg
            {
                Key = msg[startIndex].ConvertToString(),
                Sequence = BitConverter.ToInt64(msg[startIndex + 1].ToByteArray(), 0),
                UUID = new Guid(msg[startIndex + 2].ToByteArray()),
                Body = msg[startIndex + 3].ToByteArray()
            };
        }
    }
}
