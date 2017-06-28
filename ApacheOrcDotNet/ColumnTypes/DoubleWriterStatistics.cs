using System;
using ApacheOrcDotNet.Statistics;

namespace ApacheOrcDotNet.ColumnTypes
{
    public class DoubleWriterStatistics : ColumnWriterStatistics, IStatistics
    {
        public double Min { get; set; } = double.MaxValue;
        public double Max { get; set; } = double.MinValue;
        public double? Sum { get; set; } = 0;
        public ulong NumValues { get; set; }
        public bool HasNull { get; set; }

        public void FillColumnStatistics(ColumnStatistics columnStatistics)
        {
            if (columnStatistics.DoubleStatistics == null)
            {
                columnStatistics.DoubleStatistics = new DoubleStatistics
                {
                    Minimum = Min,
                    Maximum = Max,
                    Sum = Sum
                };
            }
            else
            {
                if (Min < columnStatistics.DoubleStatistics.Minimum)
                    columnStatistics.DoubleStatistics.Minimum = Min;
                if (Max > columnStatistics.DoubleStatistics.Maximum)
                    columnStatistics.DoubleStatistics.Maximum = Max;
                columnStatistics.DoubleStatistics.Sum = !Sum.HasValue
                    ? null
                    : CheckedAdd(columnStatistics.DoubleStatistics.Sum, Sum.Value);
            }

            columnStatistics.NumberOfValues += NumValues;
            if (HasNull)
                columnStatistics.HasNull = true;
        }

        public void AddValue(double? value)
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

        private double? CheckedAdd(double? left, double right)
        {
            if (!left.HasValue)
                return null;

            try
            {
                return left.Value + right;
            }
            catch (OverflowException)
            {
                return null;
            }
        }
    }
}