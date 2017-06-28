﻿using ApacheOrcDotNet.Statistics;

namespace ApacheOrcDotNet.ColumnTypes
{
    public class TimestampWriterStatistics : ColumnWriterStatistics, IStatistics
    {
        public long Min { get; set; } = long.MaxValue;
        public long Max { get; set; } = long.MinValue;
        public ulong NumValues { get; set; }
        public bool HasNull { get; set; }

        public void FillColumnStatistics(ColumnStatistics columnStatistics)
        {
            if (columnStatistics.TimestampStatistics == null)
            {
                columnStatistics.TimestampStatistics = new TimestampStatistics
                {
                    Minimum = Min,
                    Maximum = Max
                };
            }
            else
            {
                if (Min < columnStatistics.TimestampStatistics.Minimum)
                    columnStatistics.TimestampStatistics.Minimum = Min;
                if (Max > columnStatistics.TimestampStatistics.Maximum)
                    columnStatistics.TimestampStatistics.Maximum = Max;
            }

            columnStatistics.NumberOfValues += NumValues;
            if (HasNull)
                columnStatistics.HasNull = true;
        }

        public void AddValue(long? millisecondsSinceUnixEpoch)
        {
            if (!millisecondsSinceUnixEpoch.HasValue)
            {
                HasNull = true;
            }
            else
            {
                if (millisecondsSinceUnixEpoch > Max)
                    Max = millisecondsSinceUnixEpoch.Value;
                if (millisecondsSinceUnixEpoch < Min)
                    Min = millisecondsSinceUnixEpoch.Value;
            }
            NumValues++;
        }
    }
}