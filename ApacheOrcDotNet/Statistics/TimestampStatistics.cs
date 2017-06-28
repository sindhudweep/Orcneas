using System;
using ProtoBuf;

namespace ApacheOrcDotNet.Statistics
{
    [ProtoContract]
    public class TimestampStatistics : IDateTimeStatistics
    {
        private DateTime Epoch { get; } = new DateTime(2015, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        [ProtoMember(1, DataFormat = DataFormat.ZigZag)]
        public long Minimum { get; set; }

        [ProtoMember(2, DataFormat = DataFormat.ZigZag)]
        public long Maximum { get; set; }

        DateTime IDateTimeStatistics.Minimum => Epoch.AddTicks(Minimum * TimeSpan.TicksPerMillisecond);

        DateTime IDateTimeStatistics.Maximum => Epoch.AddTicks(Maximum * TimeSpan.TicksPerMillisecond);
    }
}