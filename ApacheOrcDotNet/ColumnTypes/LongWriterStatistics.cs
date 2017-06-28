using System;
using ApacheOrcDotNet.Statistics;

namespace ApacheOrcDotNet.ColumnTypes
{
    public class LongWriterStatistics : ColumnWriterStatistics, IStatistics
    {
        public long Min { get; set; } = long.MaxValue;
        public long Max { get; set; } = long.MinValue;
        public long? Sum { get; set; } = 0;
        public ulong NumValues { get; set; }
        public bool HasNull { get; set; }

        public void FillColumnStatistics(ColumnStatistics columnStatistics)
        {
            if (columnStatistics.IntStatistics == null)
            {
                columnStatistics.IntStatistics = new IntegerStatistics
                {
                    Mimumum = Min,
                    Maximum = Max,
                    Sum = Sum
                };
            }
            else
            {
                if (Min < columnStatistics.IntStatistics.Mimumum)
                    columnStatistics.IntStatistics.Mimumum = Min;
                if (Max > columnStatistics.IntStatistics.Maximum)
                    columnStatistics.IntStatistics.Maximum = Max;
                columnStatistics.IntStatistics.Sum = !Sum.HasValue
                    ? null
                    : CheckedAdd(columnStatistics.IntStatistics.Sum, Sum.Value);
            }

            columnStatistics.NumberOfValues += NumValues;
            if (HasNull)
                columnStatistics.HasNull = true;
        }

        public void AddValue(long? value)
        {
            if (!value.HasValue)
            {
                HasNull = true;
            }
            else
            {
                if (value > Max)
                    Max = value.Value;
                if (value < Min)
                    Min = value.Value;
                Sum = CheckedAdd(Sum, value.Value);
            }
            NumValues++;
        }

        private long? CheckedAdd(long? left, long right)
        {
            if (!left.HasValue)
                return null;

            try
            {
                checked
                {
                    return left.Value + right;
                }
            }
            catch (OverflowException)
            {
                return null;
            }
        }
    }
}