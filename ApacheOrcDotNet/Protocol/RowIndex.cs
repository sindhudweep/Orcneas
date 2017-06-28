using System.Collections.Generic;
using ProtoBuf;

namespace ApacheOrcDotNet.Protocol
{
    [ProtoContract]
    public class RowIndex
    {
        [ProtoMember(1)]
        public List<RowIndexEntry> Entry { get; } = new List<RowIndexEntry>();
    }
}