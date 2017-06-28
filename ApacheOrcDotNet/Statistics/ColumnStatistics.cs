using ProtoBuf;

namespace ApacheOrcDotNet.Statistics
{
    [ProtoContract]
    public class ColumnStatistics : IColumnStatistics
    {
        [ProtoMember(2)]
        public IntegerStatistics IntStatistics { get; set; }

        [ProtoMember(3)]
        public DoubleStatistics DoubleStatistics { get; set; }

        [ProtoMember(4)]
        public StringStatistics StringStatistics { get; set; }

        [ProtoMember(5)]
        public BucketStatistics BucketStatistics { get; set; }

        [ProtoMember(6)]
        public DecimalStatistics DecimalStatistics { get; set; }

        [ProtoMember(7)]
        public DateStatistics DateStatistics { get; set; }

        [ProtoMember(8)]
        public BinaryStatistics BinaryStatistics { get; set; }

        [ProtoMember(9)]
        public TimestampStatistics TimestampStatistics { get; set; }

        [ProtoMember(1)]
        public ulong NumberOfValues { get; set; }

        IIntegerStatistics IColumnStatistics.IntStatistics => IntStatistics;
        IDoubleStatistics IColumnStatistics.DoubleStatistics => DoubleStatistics;
        IStringStatistics IColumnStatistics.StringStatistics => StringStatistics;
        public IBooleanStatistics BooleanStatistics => BucketStatistics;
        IDecimalStatistics IColumnStatistics.DecimalStatistics => DecimalStatistics;
        IDateTimeStatistics IColumnStatistics.DateStatistics => DateStatistics;
        IBinaryStatistics IColumnStatistics.BinaryStatistics => BinaryStatistics;
        IDateTimeStatistics IColumnStatistics.TimestampStatistics => TimestampStatistics;

        [ProtoMember(10)]
        public bool HasNull { get; set; }
    }
}