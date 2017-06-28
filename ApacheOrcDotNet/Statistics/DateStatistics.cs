using System;
using ProtoBuf;

namespace ApacheOrcDotNet.Statistics
{
    [ProtoContract]
    public class DateStatistics : IDateTimeStatistics
    {
        private DateTime Epoch { get; } = new DateTime(2015, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        [ProtoMember(1, DataFormat = DataFormat.ZigZag)]
        public int Minimum { get; set; }

        [ProtoMember(2, DataFormat = DataFormat.ZigZag)]
        public int Maximum { get; set; }

        DateTime IDateTimeStatistics.Minimum => Epoch.AddTicks(Minimum * TimeSpan.TicksPerDay);

        DateTime IDateTimeStatistics.Maximum => Epoch.AddTicks(Maximum * TimeSpan.TicksPerDay);
    }
}