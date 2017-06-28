using ApacheOrcDotNet.Statistics;

namespace ApacheOrcDotNet.ColumnTypes
{
    public class BinaryWriterStatistics : ColumnWriterStatistics, IStatistics
    {
        public long Sum { get; set; }
        public ulong NumValues { get; set; }
        public bool HasNull { get; set; }

        public void FillColumnStatistics(ColumnStatistics columnStatistics)
        {
            if (columnStatistics.BinaryStatistics == null)
                columnStatistics.BinaryStatistics = new BinaryStatistics
                {
                    Sum = Sum
                };
            else
                columnStatistics.BinaryStatistics.Sum += Sum;

            columnStatistics.NumberOfValues += NumValues;
            if (HasNull)
                columnStatistics.HasNull = true;
        }

        public void AddValue(byte[] data)
        {
            if (data == null)
                HasNull = true;
            else
                Sum += data.Length;
            NumValues++;
        }
    }
}