using System.Collections.Generic;
using ApacheOrcDotNet.Statistics;

namespace ApacheOrcDotNet.ColumnTypes
{
    public class BooleanWriterStatistics : ColumnWriterStatistics, IStatistics
    {
        public ulong FalseCount { get; set; }
        public ulong TrueCount { get; set; }
        public ulong NumValues { get; set; }
        public bool HasNull { get; set; }

        public void FillColumnStatistics(ColumnStatistics columnStatistics)
        {
            if (columnStatistics.BucketStatistics == null)
            {
                columnStatistics.BucketStatistics = new BucketStatistics
                {
                    Count = new List<ulong>
                    {
                        FalseCount,
                        TrueCount
                    }
                };
            }
            else
            {
                columnStatistics.BucketStatistics.Count[0] += FalseCount;
                columnStatistics.BucketStatistics.Count[1] += TrueCount;
            }

            columnStatistics.NumberOfValues += NumValues;
            if (HasNull)
                columnStatistics.HasNull = true;
        }

        public void AddValue(bool? value)
        {
            if (!value.HasValue)
            {
                HasNull = true;
            }
            else
            {
                if (value.Value)
                    TrueCount++;
                else
                    FalseCount++;
            }
            NumValues++;
        }
    }
}