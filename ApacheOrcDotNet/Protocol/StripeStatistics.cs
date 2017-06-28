using System.Collections.Generic;
using ApacheOrcDotNet.Statistics;
using ProtoBuf;

namespace ApacheOrcDotNet.Protocol
{
    [ProtoContract]
    public class StripeStatistics
    {
        [ProtoMember(1)]
        public List<ColumnStatistics> ColStats { get; } = new List<ColumnStatistics>();
    }
}