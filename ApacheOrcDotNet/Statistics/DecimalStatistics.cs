using ProtoBuf;

namespace ApacheOrcDotNet.Statistics
{
    [ProtoContract]
    public class DecimalStatistics : IDecimalStatistics
    {
        [ProtoMember(1)]
        public string Minimum { get; set; }

        [ProtoMember(2)]
        public string Maximum { get; set; }

        [ProtoMember(3)]
        public string Sum { get; set; }

        decimal IDecimalStatistics.Minimum => decimal.Parse(Minimum);
        decimal IDecimalStatistics.Maximum => decimal.Parse(Maximum);
        decimal IDecimalStatistics.Sum => decimal.Parse(Sum);
    }
}