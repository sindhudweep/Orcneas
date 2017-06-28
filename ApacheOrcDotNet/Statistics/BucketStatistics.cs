using System.Collections.Generic;
using ProtoBuf;

namespace ApacheOrcDotNet.Statistics
{
    [ProtoContract]
    public class BucketStatistics : IBooleanStatistics
    {
        [ProtoMember(1, IsPacked = true)]
        public List<ulong> Count { get; set; } = new List<ulong>();

        public ulong FalseCount
        {
            get
            {
                if (Count.Count > 0)
                    return Count[0];
                return 0;
            }
        }

        public ulong TrueCount
        {
            get
            {
                if (Count.Count > 1)
                    return Count[1];
                return 0;
            }
        }
    }
}