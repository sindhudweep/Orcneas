﻿using System;
using ApacheOrcDotNet.Statistics;

namespace ApacheOrcDotNet.ColumnTypes
{
    public class DecimalWriterStatistics : ColumnWriterStatistics, IStatistics
    {
        public decimal Min { get; set; } = decimal.MaxValue;
        public decimal Max { get; set; } = decimal.MinValue;
        public decimal? Sum { get; set; } = 0;
        public ulong NumValues { get; set; }
        public bool HasNull { get; set; }

        public void FillColumnStatistics(ColumnStatistics columnStatistics)
        {
            if (columnStatistics.DecimalStatistics == null)
            {
                columnStatistics.DecimalStatistics = new DecimalStatistics
                {
                    Minimum = Min.ToString(),
                    Maximum = Max.ToString(),
                    Sum = Sum.HasValue ? Sum.Value.ToString() : ""
                };
            }
            else
            {
                if (Min < decimal.Parse(columnStatistics.DecimalStatistics.Minimum))
                    columnStatistics.DecimalStatistics.Minimum = Min.ToString();
                if (Max > decimal.Parse(columnStatistics.DecimalStatistics.Maximum))
                    columnStatistics.DecimalStatistics.Maximum = Max.ToString();
                columnStatistics.DecimalStatistics.Sum = !Sum.HasValue
                    ? ""
                    : CheckedAdd(decimal.Parse(columnStatistics.DecimalStatistics.Sum), Sum.Value).ToString();
            }

            columnStatistics.NumberOfValues += NumValues;
            if (HasNull)
                columnStatistics.HasNull = true;
        }

        public void AddValue(decimal? value)
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

        private decimal? CheckedAdd(decimal? left, decimal right)
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