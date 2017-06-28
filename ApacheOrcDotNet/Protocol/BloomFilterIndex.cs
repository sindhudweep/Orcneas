using System.Collections.Generic;
using ProtoBuf;

namespace ApacheOrcDotNet.Protocol
{
    [ProtoContract]
    public class BloomFilterIndex
    {
        [ProtoMember(1)]
        public List<BloomFilter> BloomFilter { get; } = new List<BloomFilter>();
    }
}